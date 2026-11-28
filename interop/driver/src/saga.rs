//! The cross-language saga scenarios: a saga started by one language is continued by the other
//! on the shared store, the dispatch lease excludes the other language's relay, a deadline
//! scheduled by one fires on the other, and a command given up in one language produces its local
//! notice in the other. The probe commands read the shared saga ports and the local notice
//! ledgers; every wait is a bounded poll.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{Value, json};
use snodra_conformance::peer::Peer;
use snodra_conformance::wire;

use crate::Res;
use crate::call;
use crate::command;
use crate::stores::{self, Owner};

/// The saga type and association both languages register.
const SAGA_TYPE: &str = "interop-saga";
const SAGA_KEY: &str = "orderId";
/// The command that fails permanently, producing an INVALID notice.
const FAIL_TYPE: &str = "orders.interop.SagaFail";
/// The command whose handler holds the dispatch lease.
const SLOW_TYPE: &str = "orders.interop.SagaSlow";
/// The command the slow handler sends to the driver's probe service.
const SLOW_STARTED_TYPE: &str = "store.runner.SlowStarted";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaStateReply {
    instance: String,
    found: bool,
    #[serde(default)]
    saga_id: Option<String>,
    #[serde(default)]
    state: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaPublishReply {
    instance: String,
    published: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaDispatchReply {
    instance: String,
    pending: bool,
    #[serde(default)]
    #[serde(rename = "type")]
    entry_type: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaLeaseReply {
    instance: String,
    taken: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaReleaseReply {
    instance: String,
    released: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SagaNoticeReply {
    instance: String,
    language: String,
    ordinary: i64,
    published: i64,
}

/// Runs the five saga scenarios and returns one detail line per scenario.
pub async fn run(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
) -> Res<Vec<String>> {
    let mut lines = Vec::new();

    let line = continuation(peer, keys, languages, run, "postgres", "go", "python").await?;
    lines.push(line);
    let line = continuation(peer, keys, languages, run, "mongo", "python", "go").await?;
    lines.push(line);
    let line = dispatch_lease(peer, keys, languages, run, "postgres", "go", "python").await?;
    lines.push(line);
    let line = deadline(peer, keys, languages, run, "mongo", "go").await?;
    lines.push(line);
    let line = command_failed(peer, keys, languages, run, "postgres", "go", "python").await?;
    lines.push(line);

    Ok(lines)
}

/// A saga started on `starter`'s partition is continued by `continuer`'s instance: the state both
/// languages share records who started and who advanced it.
async fn continuation(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
    family: &str,
    starter: &str,
    continuer: &str,
) -> Res<String> {
    let (order, saga) = start_and_advance(
        peer, keys, languages, run, family, starter, continuer, "continue", "continue",
    )
    .await?;
    let state = saga
        .state
        .as_ref()
        .ok_or_else(|| format!("{family}: the saga has no state"))?;
    Ok(format!(
        "{starter} started and {continuer} continued {SAGA_TYPE} {} on {family} (order {order}, advances {})",
        saga.saga_id.clone().unwrap_or_default(),
        number(state, "advances")
    ))
}

/// A slow command holds the dispatch lease: the other language's probe can not take it while the
/// relay works, and can after the entry was sent.
async fn dispatch_lease(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
    family: &str,
    starter: &str,
    recorder: &str,
) -> Res<String> {
    let (order, saga) = start_and_advance(
        peer, keys, languages, run, family, starter, recorder, "lease", "lease",
    )
    .await?;
    let saga_id = saga
        .saga_id
        .clone()
        .ok_or_else(|| format!("{family}: the saga has no id"))?;
    let state = saga
        .state
        .as_ref()
        .ok_or_else(|| format!("{family}: the saga has no state"))?;
    if text(state, "slowRecordedBy") != Some(recorder) {
        return Err(format!(
            "{family}: the slow command was not recorded by {recorder}: {state}"
        ));
    }

    let noticed = wait_notification(peer, &saga_id, Duration::from_secs(30)).await?;
    let holder = noticed
        .get("language")
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{family}: the slow notification named no language: {noticed}"))?;
    let other = languages
        .iter()
        .find(|language| language.as_str() != holder)
        .ok_or_else(|| format!("{family}: the slow language {holder} is not in {languages:?}"))?
        .clone();
    let other_instance = stores::instance(&other, family);

    let dispatch = saga_dispatch(peer, keys, &other_instance, family, run, &saga_id).await?;
    if !dispatch.pending || dispatch.entry_type.as_deref() != Some(SLOW_TYPE) {
        return Err(format!(
            "{family}: the slow command is not the other language's pending entry: {dispatch:?}"
        ));
    }
    let owner = format!("interop-probe-{run}-{saga_id}");
    let lease = saga_lease(
        peer,
        keys,
        &other_instance,
        family,
        run,
        &saga_id,
        &owner,
        now_ms() + 10_000,
    )
    .await?;
    if lease.taken {
        return Err(format!(
            "{family}: {other}'s probe took the dispatch lease while {holder}'s relay held it"
        ));
    }

    wait_dispatch_sent(
        peer,
        keys,
        &other_instance,
        family,
        run,
        &saga_id,
        Duration::from_secs(30),
    )
    .await?;
    let released = wait_lease_free(
        peer,
        keys,
        &other_instance,
        family,
        run,
        &saga_id,
        &owner,
        Duration::from_secs(15),
    )
    .await?;
    if !released {
        return Err(format!(
            "{family}: the dispatch lease was never free after the slow entry"
        ));
    }

    let notifications = peer
        .seen()
        .into_iter()
        .filter(|seen| {
            seen.r#type == SLOW_STARTED_TYPE
                && wire::json(&seen.payload)
                    .get("sagaId")
                    .and_then(Value::as_str)
                    == Some(&saga_id)
        })
        .count();
    if notifications != 1 {
        return Err(format!(
            "{family}: the slow handler ran {notifications} times (expected once) for {saga_id}"
        ));
    }
    Ok(format!(
        "the dispatch lease of {saga_id} on {family} was held by {holder}'s relay while {other}'s probe was refused, and was free after the entry was sent (order {order})"
    ))
}

/// A saga started on `starter`'s partition schedules a deadline; the scenario retries fresh sagas
/// until the scheduler of the other language fires it.
async fn deadline(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
    family: &str,
    starter: &str,
) -> Res<String> {
    let mut last = String::from("no saga");
    for attempt in 0..6 {
        let owners = stores::ready(peer, keys, languages, family, run).await?;
        let (start_partition, _) = partition_owned(&owners, starter)?;
        let start_key = stores::key_on_partition(
            family,
            run,
            &start_partition,
            &format!("saga-deadline-{starter}-{attempt}"),
        )?;
        let order = format!("saga-{run}-{family}-deadline-{starter}-{attempt}");
        let start_id = format!("saga-start-{run}-{family}-deadline-{attempt}");
        publish(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &start_id,
            &start_key,
            "started",
            &order,
            "deadline",
        )
        .await?;
        let state = wait_saga(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &order,
            Duration::from_secs(15),
            |state| text(state, "startedBy") == Some(starter),
        )
        .await?;
        if text(&state, "startedBy") != Some(starter) {
            last = format!("started by {:?}", text(&state, "startedBy"));
            continue;
        }
        let fired = wait_saga(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &order,
            Duration::from_secs(20),
            |state| flag(state, "deadlineFired"),
        )
        .await?;
        let fired_by = text(&fired, "deadlineFiredBy").unwrap_or_default();
        if fired_by != starter {
            return Ok(format!(
                "a deadline scheduled by {starter} in {order} on {family} fired in {fired_by} (scheduled by {})",
                text(&fired, "deadlineScheduledBy").unwrap_or_default()
            ));
        }
        last = format!("fired by {fired_by}");
    }
    Err(format!(
        "{family}: no deadline scheduled by {starter} fired in another language within 6 sagas ({last})"
    ))
}

/// A command recorded by `recorder` fails permanently; the scenario retries fresh sagas until the
/// notice is delivered by the other language, locally, and asserts the notice's fields and the
/// absence of an event-store delivery.
async fn command_failed(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
    family: &str,
    starter: &str,
    recorder: &str,
) -> Res<String> {
    let mut last = String::from("no notice");
    for attempt in 0..6 {
        let (order, saga) = start_and_advance(
            peer,
            keys,
            languages,
            run,
            family,
            starter,
            recorder,
            "notice",
            &format!("notice-{attempt}"),
        )
        .await?;
        let saga_id = saga
            .saga_id
            .clone()
            .ok_or_else(|| format!("{family}: the saga has no id"))?;
        let state = wait_saga(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &order,
            Duration::from_secs(30),
            |state| number(state, "notices") >= 1,
        )
        .await?;
        let notice_by = text(&state, "noticeBy").unwrap_or_default();
        if notice_by == recorder {
            last = format!("notice by {notice_by}, the recording language");
            continue;
        }
        let reason = text(&state, "noticeReason").unwrap_or_default();
        let command_type = text(&state, "noticeCommandType").unwrap_or_default();
        let key = text(&state, "noticeKey").unwrap_or_default();
        if reason != "INVALID"
            || command_type != FAIL_TYPE
            || !key.starts_with(&format!("{saga_id}:"))
        {
            return Err(format!(
                "{family}: the notice is not the expected INVALID of {FAIL_TYPE}: {state}"
            ));
        }
        let notice_id = format!("failed:{key}");
        let first = saga_notice(
            peer,
            keys,
            &stores::instance(languages[0].as_str(), family),
            family,
            run,
            &saga_id,
            &notice_id,
        )
        .await?;
        let second = saga_notice(
            peer,
            keys,
            &stores::instance(languages[1].as_str(), family),
            family,
            run,
            &saga_id,
            &notice_id,
        )
        .await?;
        let ordinary = first.ordinary + second.ordinary;
        if ordinary != 1 {
            return Err(format!(
                "{family}: the local notice ran {ordinary} ordinary handlers (expected one): {first:?} {second:?}"
            ));
        }
        let delivered = if first.ordinary == 1 { &first } else { &second };
        if delivered.language.as_str() != notice_by || first.published != 0 || second.published != 0
        {
            return Err(format!(
                "{family}: the notice was not local to {notice_by} ({first:?} {second:?})"
            ));
        }
        return Ok(format!(
            "{recorder} recorded a failing command in saga {saga_id} and {notice_by} delivered its local INVALID notice on {family} (ordinary handler once, no event-store publication)"
        ));
    }
    Err(format!(
        "{family}: no notice was delivered by a language other than {recorder} within 6 sagas ({last})"
    ))
}

/// Starts a saga on `starter`'s partition and continues it on `advancer`'s, retrying fresh sagas
/// whose events landed elsewhere. Returns the order, the state and the owners.
#[allow(clippy::too_many_arguments)] // the scenario's inputs, all named at the call site
async fn start_and_advance(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
    family: &str,
    starter: &str,
    advancer: &str,
    mode: &str,
    salt: &str,
) -> Res<(String, SagaStateReply)> {
    let mut last = String::from("no events");
    for attempt in 0..3 {
        let owners = stores::ready(peer, keys, languages, family, run).await?;
        let (start_partition, _) = partition_owned(&owners, starter)?;
        let (advance_partition, _) = partition_owned(&owners, advancer)?;
        let start_key = stores::key_on_partition(
            family,
            run,
            &start_partition,
            &format!("saga-start-{starter}-{salt}-{attempt}"),
        )?;
        let advance_key = stores::key_on_partition(
            family,
            run,
            &advance_partition,
            &format!("saga-advance-{advancer}-{salt}-{attempt}"),
        )?;
        let order = format!("saga-{run}-{family}-{starter}-{salt}-{attempt}");
        let start_id = format!("saga-start-{run}-{family}-{starter}-{salt}-{attempt}");
        publish(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &start_id,
            &start_key,
            "started",
            &order,
            mode,
        )
        .await?;
        let state = wait_saga(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &order,
            Duration::from_secs(15),
            |_| true,
        )
        .await?;
        if text(&state, "startedBy") != Some(starter) {
            last = format!("started by {:?}", text(&state, "startedBy"));
            continue;
        }
        let advance_id = format!("saga-advance-{run}-{family}-{advancer}-{salt}-{attempt}");
        publish(
            peer,
            keys,
            &stores::instance(advancer, family),
            family,
            run,
            &advance_id,
            &advance_key,
            "advanced",
            &order,
            mode,
        )
        .await?;
        let state = wait_saga(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &order,
            Duration::from_secs(15),
            |state| number(state, "advances") >= 1,
        )
        .await?;
        if text(&state, "advancedBy") != Some(advancer) {
            last = format!("advanced by {:?}", text(&state, "advancedBy"));
            continue;
        }
        let reply = saga_state(
            peer,
            keys,
            &stores::instance(starter, family),
            family,
            run,
            &order,
        )
        .await?
        .ok_or_else(|| format!("{family}: the saga of {order} disappeared after the advance"))?;
        return Ok((order, reply));
    }
    Err(format!(
        "{family}: {starter} never started and {advancer} never continued a saga in 3 attempts ({last})"
    ))
}

fn partition_owned(owners: &BTreeMap<String, Owner>, language: &str) -> Res<(String, Owner)> {
    owners
        .iter()
        .find(|(_, owner)| owner.language == language)
        .map(|(partition, owner)| (partition.clone(), owner.clone()))
        .ok_or_else(|| format!("{language} never owned a partition: {owners:?}"))
}

async fn publish(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    id: &str,
    key: &str,
    kind: &str,
    order: &str,
    mode: &str,
) -> Res<()> {
    let routing = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.SagaPublish",
            Some(routing),
            json!({
                "family": family, "run": run, "id": id, "key": key,
                "kind": kind, "orderId": order, "mode": mode,
            }),
        ),
    )
    .await?;
    let reply: SagaPublishReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: SagaPublish answered something else: {e}"))?;
    if reply.instance != instance || reply.published != id {
        return Err(format!(
            "{instance}: SagaPublish did not publish {id}: {reply:?}"
        ));
    }
    Ok(())
}

async fn saga_state(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    order: &str,
) -> Res<Option<SagaStateReply>> {
    let routing = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.SagaState",
            Some(routing),
            json!({
                "family": family, "run": run, "type": SAGA_TYPE,
                "key": SAGA_KEY, "value": order, "sagaId": "",
            }),
        ),
    )
    .await?;
    let reply: SagaStateReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: SagaState answered something else: {e}"))?;
    if reply.instance != instance {
        return Err(format!(
            "SagaState for {instance} reached {}: the membership moved",
            reply.instance
        ));
    }
    Ok(reply.found.then_some(reply))
}

async fn wait_saga(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    order: &str,
    limit: Duration,
    ok: impl Fn(&Value) -> bool,
) -> Res<Value> {
    let deadline = tokio::time::Instant::now() + limit;
    let mut last = String::from("no saga");
    loop {
        if let Some(reply) = saga_state(peer, keys, instance, family, run, order).await? {
            if let Some(state) = &reply.state
                && ok(state)
            {
                return Ok(state.clone());
            }
            last = format!("state {:?}", reply.state);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{family}: the saga of {order} did not reach the expected state within {limit:?} ({last})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn saga_dispatch(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    saga_id: &str,
) -> Res<SagaDispatchReply> {
    let routing = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.SagaDispatch",
            Some(routing),
            json!({"family": family, "run": run, "sagaId": saga_id}),
        ),
    )
    .await?;
    let reply: SagaDispatchReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: SagaDispatch answered something else: {e}"))?;
    if reply.instance != instance {
        return Err(format!(
            "SagaDispatch for {instance} reached {}",
            reply.instance
        ));
    }
    Ok(reply)
}

async fn wait_dispatch_sent(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    saga_id: &str,
    limit: Duration,
) -> Res<()> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let dispatch = saga_dispatch(peer, keys, instance, family, run, saga_id).await?;
        if !dispatch.pending {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "{family}: {saga_id} still has a pending entry {dispatch:?} after {limit:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn saga_lease(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    saga_id: &str,
    owner: &str,
    until_ms: i64,
) -> Res<SagaLeaseReply> {
    let routing = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.SagaLease",
            Some(routing),
            json!({
                "family": family, "run": run, "sagaId": saga_id,
                "owner": owner, "untilMs": until_ms,
            }),
        ),
    )
    .await?;
    let reply: SagaLeaseReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: SagaLease answered something else: {e}"))?;
    if reply.instance != instance {
        return Err(format!(
            "SagaLease for {instance} reached {}",
            reply.instance
        ));
    }
    Ok(reply)
}

async fn saga_release(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    saga_id: &str,
    owner: &str,
) -> Res<()> {
    let routing = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.SagaRelease",
            Some(routing),
            json!({"family": family, "run": run, "sagaId": saga_id, "owner": owner}),
        ),
    )
    .await?;
    let reply: SagaReleaseReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: SagaRelease answered something else: {e}"))?;
    if reply.instance != instance || !reply.released {
        return Err(format!(
            "{instance}: SagaRelease did not release: {reply:?}"
        ));
    }
    Ok(())
}

async fn wait_lease_free(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    saga_id: &str,
    owner: &str,
    limit: Duration,
) -> Res<bool> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        let lease = saga_lease(
            peer,
            keys,
            instance,
            family,
            run,
            saga_id,
            owner,
            now_ms() + 10_000,
        )
        .await?;
        if lease.taken {
            saga_release(peer, keys, instance, family, run, saga_id, owner).await?;
            return Ok(true);
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_notification(peer: &Peer, saga_id: &str, limit: Duration) -> Res<Value> {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        for seen in peer.seen() {
            if seen.r#type == SLOW_STARTED_TYPE {
                let value = wire::json(&seen.payload);
                if value.get("sagaId").and_then(Value::as_str) == Some(saga_id) {
                    return Ok(value);
                }
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "no slow-command notification for {saga_id} within {limit:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn saga_notice(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    family: &str,
    run: &str,
    saga_id: &str,
    notice_id: &str,
) -> Res<SagaNoticeReply> {
    let routing = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.SagaNotice",
            Some(routing),
            json!({
                "family": family, "run": run, "sagaId": saga_id, "noticeId": notice_id,
            }),
        ),
    )
    .await?;
    let reply: SagaNoticeReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: SagaNotice answered something else: {e}"))?;
    if reply.instance != instance {
        return Err(format!(
            "SagaNotice for {instance} reached {}",
            reply.instance
        ));
    }
    Ok(reply)
}

fn text<'a>(state: &'a Value, name: &str) -> Option<&'a str> {
    state.get(name).and_then(Value::as_str)
}

fn number(state: &Value, name: &str) -> i64 {
    state.get(name).and_then(Value::as_i64).unwrap_or(0)
}

fn flag(state: &Value, name: &str) -> bool {
    state.get(name).and_then(Value::as_bool).unwrap_or(false)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
