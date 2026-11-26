//! The `stores` subcommand: proves the shared brokers and stores across languages.
//!
//! One `store-interop` instance per language and store family is registered on the hub; the
//! driver addresses an instance with a routing key and reads what its consumer delivered. The
//! scenarios are:
//!
//! 1. one Kafka consumer group (family `kafka`) shared by the languages, each reading the other's
//!    events exactly once;
//! 2. one RabbitMQ Streams consumer name with single-active-consumer (family `rabbitmq`), the same;
//! 3. the outbox over PostgreSQL, MySQL and MongoDB: a language appends, both relay and consume,
//!    and each language reads the other's events;
//! 4. a processed-event marker written by one language skips the duplicate delivered to the other
//!    on each of those three stores.
//!
//! Names carry the run token: the sources are `snodra.interop.<run>.<family>` and the shared
//! databases are `snodra_interop_<run>`. The harness's compose file creates them; `run.sh`
//! removes everything it started.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use clap::Args as ClapArgs;
use serde::Deserialize;
use serde_json::json;

use snodra_conformance::peer::{Peer, Registration, topology};

use crate::{Res, call, command};

/// The service every probe registers.
const SERVICE: &str = "store-interop";
/// The store families, in scenario order.
const FAMILIES: [&str; 5] = ["kafka", "rabbitmq", "postgres", "mysql", "mongo"];
/// The families whose message store both languages share.
const SHARED: [&str; 3] = ["postgres", "mysql", "mongo"];
/// The partitions of every topic and super stream: one per language.
const PARTITIONS: usize = 2;
/// The languages the profile supports.
const LANGUAGES: [&str; 2] = ["go", "python"];

#[derive(ClapArgs)]
pub struct StoresArgs {
    /// The hub's address (host:port)
    #[arg(long)]
    hub: String,
    /// The languages that must be registered, comma-separated (`go,python`)
    #[arg(long, value_delimiter = ',')]
    languages: Vec<String>,
    /// The run token the harness generated; every source and database name carries it
    #[arg(long)]
    run: String,
    /// Exit non-zero when a scenario could not be exercised (all of them are required here)
    #[arg(long)]
    strict: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WhoReply {
    instance: String,
    language: String,
    family: String,
}

#[derive(Deserialize)]
struct PublishReply {
    instance: String,
    ids: Vec<String>,
}

#[derive(Deserialize)]
struct PublishRawReply {
    instance: String,
    published: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StateReply {
    instance: String,
    language: String,
    family: String,
    run: String,
    store: String,
    event_store: String,
    source: String,
    database: Option<String>,
    subscription: String,
    outbox_pending: i64,
    deliveries: Vec<Delivery>,
    markers: BTreeMap<String, bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Delivery {
    id: String,
    segment: String,
    outcome: String,
}

#[derive(Clone)]
struct Owner {
    instance: String,
    language: String,
}

fn instance(language: &str, family: &str) -> String {
    format!("{language}-{family}")
}

fn source_name(run: &str, family: &str) -> String {
    format!("snodra.interop.{run}.{family}")
}

fn database_name(run: &str) -> String {
    format!("snodra_interop_{run}")
}

fn subscription_name(family: &str) -> String {
    format!("{SERVICE}.interop-{family}")
}

fn is_shared(family: &str) -> bool {
    SHARED.contains(&family)
}

fn expected_store(family: &str) -> &'static str {
    match family {
        "kafka" | "rabbitmq" => "memory",
        "postgres" => "postgres",
        "mysql" => "mysql",
        "mongo" => "mongo",
        _ => "?",
    }
}

fn expected_event_store(family: &str) -> &'static str {
    if family == "rabbitmq" {
        "rabbitmq-streams"
    } else {
        "kafka"
    }
}

pub async fn run(args: &StoresArgs) -> Res<()> {
    let mut languages = args.languages.clone();
    languages.sort();
    languages.dedup();
    if languages.iter().map(String::as_str).collect::<Vec<_>>() != LANGUAGES.to_vec() {
        return Err(format!(
            "the stores profile runs {} ; the other languages' store-interop services are a follow-up \
             (got {:?})",
            LANGUAGES.join(" and "),
            args.languages
        ));
    }
    if args.run.is_empty()
        || !args
            .run
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    {
        return Err(format!(
            "--run {:?} must be lowercase letters and digits only",
            args.run
        ));
    }
    let instances: Vec<String> = languages
        .iter()
        .flat_map(|language| {
            FAMILIES
                .iter()
                .map(move |family| instance(language, family))
        })
        .collect();
    wait_for_instances(&args.hub, &instances).await?;
    println!(
        "ok   topology: {} instances of {SERVICE} ({} x {})",
        instances.len(),
        languages.join(","),
        FAMILIES.join(",")
    );

    let peer = Peer::connect(
        &args.hub,
        Registration {
            service: "store-runner".into(),
            commands: vec!["store.runner.Probe".into()],
            ..Registration::default()
        },
        |peer, request| peer.answer(&request.id, &json!({ "probe": true })),
    )
    .await?;
    let keys = find_keys(&peer, &instances).await?;
    println!("ok   routing keys: {} instances addressed", keys.len());
    check_names(&peer, &keys, &languages, &args.run).await?;

    let mut scenarios = 0usize;
    for family in FAMILIES {
        let owners = ready(&peer, &keys, &languages, family, &args.run).await?;
        outbox_scenario(&peer, &keys, &languages, family, &args.run).await?;
        println!(
            "ok   {family}: the outbox from each language is consumed by the other through {}",
            if family == "rabbitmq" {
                "one RabbitMQ Streams consumer name (single-active-consumer)"
            } else if family == "kafka" {
                "one Kafka consumer group"
            } else {
                "the shared store"
            }
        );
        scenarios += 1;
        if is_shared(family) {
            let (first, second) =
                marker_scenario(&peer, &keys, &languages, family, &args.run, &owners).await?;
            println!(
                "ok   {family}: a marker written by {first} skipped {second}'s duplicate delivery"
            );
            scenarios += 1;
        }
    }
    println!(
        "ok   names: sources {} (run {}), one database {} for {}",
        FAMILIES
            .iter()
            .map(|family| source_name(&args.run, family))
            .collect::<Vec<_>>()
            .join(", "),
        args.run,
        database_name(&args.run),
        SHARED.join(", ")
    );
    let expected = FAMILIES.len() + SHARED.len();
    if scenarios != expected {
        return Err(format!(
            "{} of {expected} scenarios ran; the stores profile requires every one",
            scenarios
        ));
    }
    println!(
        "stores: {} languages, {} scenarios passed across {}, run {}",
        languages.len(),
        scenarios,
        FAMILIES.join(","),
        args.run
    );
    // Every scenario is required: --strict changes nothing here, but the harness's contract
    // (INTEROP_STRICT=1) passes it to both profiles, so it stays accepted.
    let _ = args.strict;
    Ok(())
}

/// Wait until the hub lists exactly the expected instances of the probe service.
async fn wait_for_instances(hub: &str, want: &[String]) -> Res<()> {
    let mut want = want.to_vec();
    want.sort();
    let mut last = Vec::new();
    for _ in 0..240 {
        if let Ok(found) = topology(hub).await {
            last = found
                .services
                .iter()
                .filter(|s| s.service_name == SERVICE)
                .flat_map(|s| s.instances.iter().map(|i| i.instance_id.clone()))
                .collect();
            last.sort();
            if last == want {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!(
        "the topology shows instances {last:?} of {SERVICE}, expected {want:?}"
    ))
}

/// A routing key per instance: a keyed `Who` whose reply names the instance.
async fn find_keys(peer: &Peer, instances: &[String]) -> Res<BTreeMap<String, String>> {
    let mut keys = BTreeMap::new();
    for n in 0..5000 {
        if keys.len() == instances.len() {
            break;
        }
        let key = format!("key-{n}");
        let reply = call(peer, command("orders.interop.Who", Some(&key), json!({}))).await?;
        let who: WhoReply = serde_json::from_value(reply)
            .map_err(|e| format!("Who answered something else: {e}"))?;
        if who.family.is_empty() || who.language.is_empty() {
            return Err(format!("Who answered {who:?}"));
        }
        let instance = instance(&who.language, &who.family);
        if instance != who.instance {
            return Err(format!(
                "Who answered {} for language {} and family {}",
                who.instance, who.language, who.family
            ));
        }
        if instances.contains(&instance) {
            keys.entry(instance).or_insert(key);
        }
    }
    for instance in instances {
        if !keys.contains_key(instance) {
            return Err(format!(
                "no routing key of key-0..key-4999 reaches {instance}"
            ));
        }
    }
    Ok(keys)
}

/// The identity and names of every instance: the run, the sources and the one shared database.
async fn check_names(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    run: &str,
) -> Res<()> {
    for language in languages {
        for family in FAMILIES {
            let instance = instance(language, family);
            let state = state(peer, keys, &instance, &[]).await?;
            let expected_database = if is_shared(family) {
                Some(database_name(run))
            } else {
                None
            };
            let checks = [
                (state.instance == instance, "instance"),
                (state.language == *language, "language"),
                (state.family == family, "family"),
                (state.run == run, "run"),
                (state.source == source_name(run, family), "source"),
                (
                    state.subscription == subscription_name(family),
                    "subscription",
                ),
                (state.store == expected_store(family), "store"),
                (
                    state.event_store == expected_event_store(family),
                    "event store",
                ),
                (state.database == expected_database, "database"),
            ];
            for (ok, what) in checks {
                if !ok {
                    return Err(format!(
                        "{instance}: the {what} is not the run's: {state:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// One instance's state, checking that the key routed to it.
async fn state(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    markers: &[String],
) -> Res<StateReply> {
    let key = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.State",
            Some(key),
            json!({ "markers": markers }),
        ),
    )
    .await?;
    let state: StateReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: State answered something else: {e}"))?;
    if state.instance != instance {
        return Err(format!(
            "the key of {instance} reached {}: the membership moved",
            state.instance
        ));
    }
    Ok(state)
}

/// The state of every language in one family.
async fn states(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    family: &str,
    markers: &[String],
) -> Res<Vec<StateReply>> {
    let mut out = Vec::with_capacity(languages.len());
    for language in languages {
        out.push(state(peer, keys, &instance(language, family), markers).await?);
    }
    Ok(out)
}

/// The ids the `Publish` command appended to the outbox.
async fn publish(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    run: &str,
    family: &str,
    publisher: &str,
    event_keys: &[String],
) -> Res<Vec<String>> {
    let key = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.Publish",
            Some(key),
            json!({
                "family": family,
                "publisher": publisher,
                "run": run,
                "count": event_keys.len(),
                "keys": event_keys,
            }),
        ),
    )
    .await?;
    let reply: PublishReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: Publish answered something else: {e}"))?;
    if reply.instance != instance {
        return Err(format!(
            "the key of {instance} reached {}: the membership moved",
            reply.instance
        ));
    }
    if reply.ids.len() != event_keys.len() {
        return Err(format!(
            "{instance}: published {} of {} events",
            reply.ids.len(),
            event_keys.len()
        ));
    }
    Ok(reply.ids)
}

/// The ids the `PublishRaw` command put into the event store.
#[allow(clippy::too_many_arguments)] // the scenario's inputs, all named at the call site
async fn publish_raw(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    instance: &str,
    run: &str,
    family: &str,
    publisher: &str,
    ids: &[String],
    event_keys: &[String],
) -> Res<Vec<String>> {
    let key = keys
        .get(instance)
        .ok_or_else(|| format!("no routing key for {instance}"))?;
    let reply = call(
        peer,
        command(
            "orders.interop.PublishRaw",
            Some(key),
            json!({
                "family": family,
                "publisher": publisher,
                "run": run,
                "ids": ids,
                "keys": event_keys,
            }),
        ),
    )
    .await?;
    let reply: PublishRawReply = serde_json::from_value(reply)
        .map_err(|e| format!("{instance}: PublishRaw answered something else: {e}"))?;
    if reply.instance != instance {
        return Err(format!(
            "the key of {instance} reached {}: the membership moved",
            reply.instance
        ));
    }
    if reply.published.len() != ids.len() {
        return Err(format!(
            "{instance}: published {} of {} events directly",
            reply.published.len(),
            ids.len()
        ));
    }
    Ok(reply.published)
}

/// Two keys that land on the two partitions, by the family's own rule (murmur2 for Kafka,
/// murmur3 with the stream seed for RabbitMQ).
fn partition_keys(family: &str, run: &str) -> [String; PARTITIONS] {
    let mut found: [Option<String>; PARTITIONS] = [None, None];
    for n in 0..10_000 {
        if found.iter().all(Option::is_some) {
            break;
        }
        let key = format!("interop-{run}-{n}");
        let partition = partition_of(family, &key, PARTITIONS);
        found[partition].get_or_insert(key);
    }
    [
        found[0].clone().expect("a key for partition 0"),
        found[1].clone().expect("a key for partition 1"),
    ]
}

fn partition_of(family: &str, key: &str, partitions: usize) -> usize {
    if family == "rabbitmq" {
        (murmur3(key.as_bytes()) as usize) % partitions
    } else {
        let hash = murmur2(key.as_bytes());
        ((hash & 0x7fff_ffff) as usize) % partitions
    }
}

/// Wait until the two languages own one partition each, and return the owner of every segment.
async fn ready(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    family: &str,
    run: &str,
) -> Res<BTreeMap<String, Owner>> {
    let partitions = partition_keys(family, run);
    let publisher = instance(&languages[0], family);
    let mut last = String::from("no probe event was delivered");
    for attempt in 0..30 {
        let ids = [
            format!("ready-{run}-{family}-{attempt}-0"),
            format!("ready-{run}-{family}-{attempt}-1"),
        ];
        publish_raw(
            peer,
            keys,
            &publisher,
            run,
            family,
            "probe",
            &ids,
            &partitions,
        )
        .await?;
        let until = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let observed = states(peer, keys, languages, family, &[]).await?;
            let mut owners = BTreeMap::new();
            for state in &observed {
                for delivery in &state.deliveries {
                    if ids.contains(&delivery.id) && delivery.outcome == "handled" {
                        owners.insert(
                            delivery.segment.clone(),
                            Owner {
                                instance: state.instance.clone(),
                                language: state.language.clone(),
                            },
                        );
                    }
                }
            }
            let instances: BTreeSet<&str> = owners.values().map(|o| o.instance.as_str()).collect();
            if owners.contains_key("0") && owners.contains_key("1") && instances.len() == 2 {
                return Ok(owners);
            }
            last = format!(
                "segments {:?}, instances {:?}",
                owners.keys().collect::<Vec<_>>(),
                instances
            );
            if tokio::time::Instant::now() >= until {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    Err(format!(
        "{family}: the two languages never split the partitions ({last})"
    ))
}

/// Scenario 3: the outbox across languages. Each language appends, both relay and consume, and
/// each reads the other's events exactly once.
async fn outbox_scenario(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    family: &str,
    run: &str,
) -> Res<()> {
    let partitions = partition_keys(family, run);
    let mut by_publisher: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for language in languages {
        let instance = instance(language, family);
        let ids = publish(peer, keys, &instance, run, family, language, &partitions).await?;
        by_publisher.insert(language.clone(), ids);
    }
    let all_ids: Vec<String> = by_publisher.values().flatten().cloned().collect();
    let until = tokio::time::Instant::now() + Duration::from_secs(90);
    let observed = loop {
        let observed = states(peer, keys, languages, family, &all_ids).await?;
        let handled = all_ids
            .iter()
            .filter(|id| {
                observed.iter().any(|s| {
                    s.deliveries
                        .iter()
                        .any(|d| &d.id == *id && d.outcome == "handled")
                })
            })
            .count();
        let pending: i64 = observed.iter().map(|s| s.outbox_pending).sum();
        if handled == all_ids.len() && pending == 0 && markers_visible(family, &observed, &all_ids)
        {
            break observed;
        }
        if tokio::time::Instant::now() >= until {
            return Err(format!(
                "{family}: {handled}/{} events handled, {pending} outbox entries pending, markers {:?}",
                all_ids.len(),
                observed
                    .iter()
                    .map(|s| s.markers.clone())
                    .collect::<Vec<_>>()
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    for id in &all_ids {
        let deliveries = deliveries_of(&observed, id);
        if deliveries.len() != 1 || deliveries[0].1.outcome != "handled" {
            return Err(format!(
                "{family}: event {id} was delivered {} times: {deliveries:?}",
                deliveries.len()
            ));
        }
    }
    for (publisher, ids) in &by_publisher {
        let handlers: BTreeSet<&str> = ids
            .iter()
            .flat_map(|id| {
                observed
                    .iter()
                    .filter(move |s| {
                        s.deliveries
                            .iter()
                            .any(|d| &d.id == id && d.outcome == "handled")
                    })
                    .map(|s| s.language.as_str())
            })
            .collect();
        for language in languages {
            if !handlers.contains(language.as_str()) {
                return Err(format!(
                    "{family}: {language} never handled an event {publisher} published (handled by {handlers:?})"
                ));
            }
        }
    }
    let segments: BTreeSet<&str> = observed
        .iter()
        .flat_map(|s| s.deliveries.iter())
        .filter(|d| all_ids.contains(&d.id) && d.outcome == "handled")
        .map(|d| d.segment.as_str())
        .collect();
    if segments != BTreeSet::from(["0", "1"]) {
        return Err(format!(
            "{family}: the events were delivered on segments {segments:?}, not both partitions"
        ));
    }
    if observed.iter().any(|s| s.outbox_pending != 0) {
        return Err(format!(
            "{family}: the outbox did not drain: {:?}",
            observed
                .iter()
                .map(|s| s.outbox_pending)
                .collect::<Vec<_>>()
        ));
    }
    Ok(())
}

/// 4. A marker written by one language makes the other language's copy a duplicate.
async fn marker_scenario(
    peer: &Peer,
    keys: &BTreeMap<String, String>,
    languages: &[String],
    family: &str,
    run: &str,
    owners: &BTreeMap<String, Owner>,
) -> Res<(String, String)> {
    let partitions = partition_keys(family, run);
    let owner0 = owners
        .get("0")
        .expect("readiness names the owner of segment 0");
    let owner1 = owners
        .get("1")
        .expect("readiness names the owner of segment 1");
    if owner0.instance == owner1.instance {
        return Err(format!(
            "{family}: both partitions belong to {}",
            owner0.instance
        ));
    }
    let id = format!("dup-{run}-{family}");
    publish_raw(
        peer,
        keys,
        &owner0.instance,
        run,
        family,
        &owner0.language,
        std::slice::from_ref(&id),
        std::slice::from_ref(&partitions[0]),
    )
    .await?;
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let observed = states(peer, keys, languages, family, std::slice::from_ref(&id)).await?;
        let handled = deliveries_of(&observed, &id)
            .iter()
            .filter(|(_, d)| d.outcome == "handled")
            .count();
        let visible = observed.iter().all(|s| s.markers.get(&id) == Some(&true));
        if handled == 1 && visible {
            break;
        }
        if tokio::time::Instant::now() >= until {
            return Err(format!(
                "{family}: the first copy was handled by {handled} instance(s), markers visible {visible}: {observed:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let observed = states(peer, keys, languages, family, std::slice::from_ref(&id)).await?;
    if deliveries_of(&observed, &id)
        .iter()
        .any(|(state, _)| state.instance == owner1.instance)
    {
        return Err(format!(
            "{family}: the other language saw the event before the duplicate was published"
        ));
    }
    publish_raw(
        peer,
        keys,
        &owner1.instance,
        run,
        family,
        &owner1.language,
        std::slice::from_ref(&id),
        std::slice::from_ref(&partitions[1]),
    )
    .await?;
    let until = tokio::time::Instant::now() + Duration::from_secs(60);
    let observed = loop {
        let observed = states(peer, keys, languages, family, std::slice::from_ref(&id)).await?;
        let duplicate = deliveries_of(&observed, &id)
            .iter()
            .any(|(state, d)| state.instance == owner1.instance && d.outcome == "duplicate");
        if duplicate {
            break observed;
        }
        if tokio::time::Instant::now() >= until {
            return Err(format!(
                "{family}: the duplicate was not skipped by {}: {observed:?}",
                owner1.instance
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let first = deliveries_of(&observed, &id)
        .into_iter()
        .filter(|(state, _)| state.instance == owner0.instance)
        .collect::<Vec<_>>();
    let second = deliveries_of(&observed, &id)
        .into_iter()
        .filter(|(state, _)| state.instance == owner1.instance)
        .collect::<Vec<_>>();
    if first.len() != 1
        || first[0].1.outcome != "handled"
        || second.len() != 1
        || second[0].1.outcome != "duplicate"
    {
        return Err(format!(
            "{family}: the deliveries are {:?} on {} and {:?} on {}",
            first.iter().map(|(_, d)| &d.outcome).collect::<Vec<_>>(),
            owner0.instance,
            second.iter().map(|(_, d)| &d.outcome).collect::<Vec<_>>(),
            owner1.instance
        ));
    }
    if observed.iter().any(|s| s.markers.get(&id) != Some(&true)) {
        return Err(format!(
            "{family}: the marker of {id} is not visible in every shared store: {observed:?}"
        ));
    }
    Ok((owner0.language.clone(), owner1.language.clone()))
}

/// Every delivery of one event across the instances.
fn deliveries_of<'a>(observed: &'a [StateReply], id: &str) -> Vec<(&'a StateReply, &'a Delivery)> {
    observed
        .iter()
        .flat_map(|state| {
            state
                .deliveries
                .iter()
                .filter(|delivery| delivery.id == id)
                .map(move |delivery| (state, delivery))
        })
        .collect()
}

/// Whether every event's marker is visible where the store shares it: on the handling instance
/// always, on both instances when the message store is shared.
fn markers_visible(family: &str, observed: &[StateReply], ids: &[String]) -> bool {
    for id in ids {
        let handlers: Vec<&StateReply> = observed
            .iter()
            .filter(|state| {
                state
                    .deliveries
                    .iter()
                    .any(|delivery| &delivery.id == id && delivery.outcome == "handled")
            })
            .collect();
        if handlers.len() != 1 {
            return false;
        }
        if handlers[0].markers.get(id) != Some(&true) {
            return false;
        }
        if is_shared(family)
            && observed
                .iter()
                .any(|state| state.markers.get(id) != Some(&true))
        {
            return false;
        }
    }
    true
}

/// The Kafka default partitioner's murmur2, as the Java client has it: a signed 32-bit value
/// (masked positive by the caller).
fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    let length = data.len() as u32;
    let mut value = SEED ^ length;
    let mut at = 0;
    while at + 4 <= data.len() {
        let mut word = u32::from_le_bytes([data[at], data[at + 1], data[at + 2], data[at + 3]]);
        word = word.wrapping_mul(M);
        word ^= word >> 24;
        word = word.wrapping_mul(M);
        value = value.wrapping_mul(M);
        value ^= word;
        at += 4;
    }
    let extra = data.len() - at;
    if extra >= 3 {
        value ^= u32::from(data[at + 2]) << 16;
    }
    if extra >= 2 {
        value ^= u32::from(data[at + 1]) << 8;
    }
    if extra >= 1 {
        value ^= u32::from(data[at]);
        value = value.wrapping_mul(M);
    }
    value ^= value >> 13;
    value = value.wrapping_mul(M);
    value ^= value >> 15;
    value as i32
}

/// The RabbitMQ Streams clients' murmur3 x86_32 with seed 104729, unsigned.
fn murmur3(data: &[u8]) -> u32 {
    const SEED: u32 = 104_729;
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;
    let length = data.len() as u32;
    let mut value = SEED;
    let rounds = data.len() & !3;
    let mut at = 0;
    while at < rounds {
        let mut word = u32::from_le_bytes([data[at], data[at + 1], data[at + 2], data[at + 3]]);
        word = word.wrapping_mul(C1);
        word = word.rotate_left(15);
        word = word.wrapping_mul(C2);
        value ^= word;
        value = value.rotate_left(13);
        value = value.wrapping_mul(5).wrapping_add(0xe654_6b64);
        at += 4;
    }
    let tail = data.len() & 3;
    if tail > 0 {
        let mut word = 0u32;
        if tail >= 3 {
            word ^= u32::from(data[rounds + 2]) << 16;
        }
        if tail >= 2 {
            word ^= u32::from(data[rounds + 1]) << 8;
        }
        word ^= u32::from(data[rounds]);
        word = word.wrapping_mul(C1);
        word = word.rotate_left(15);
        word = word.wrapping_mul(C2);
        value ^= word;
    }
    value ^= length;
    value ^= value >> 16;
    value = value.wrapping_mul(0x85eb_ca6b);
    value ^= value >> 13;
    value = value.wrapping_mul(0xc2b2_ae35);
    value ^= value >> 16;
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur2_matches_the_java_clients_vectors() {
        assert_eq!(murmur2(b"21"), -973_932_308);
        assert_eq!(murmur2(b"foobar"), -790_332_482);
        assert_eq!(murmur2(b"a-little-bit-long-string"), -985_981_536);
        assert_eq!(murmur2(b"a-little-bit-longer-string"), -1_486_304_829);
        assert_eq!(
            murmur2(b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8"),
            -58_897_971
        );
    }

    #[test]
    fn kafka_partitions_match_the_pinned_vectors() {
        for (key, partition) in [
            ("order-1", 4),
            ("order-2", 3),
            ("order-42", 0),
            ("customer-7", 1),
            ("", 3),
        ] {
            assert_eq!(
                ((murmur2(key.as_bytes()) & 0x7fff_ffff) as usize) % 6,
                partition,
                "{key}"
            );
            assert_eq!(partition_of("kafka", key, 6), partition, "{key}");
        }
    }

    #[test]
    fn murmur3_matches_the_stream_clients_pinned_vectors() {
        for (key, hashed, partition) in [
            ("order-1", 0x3163_ba7a, 0),
            ("order-2", 0x4aa7_bf62, 2),
            ("customer-123", 0x6fab_e837, 5),
            ("order-7", 0x813d_1adb, 3),
            ("", 0xc675_7d56, 4),
        ] {
            assert_eq!(murmur3(key.as_bytes()), hashed, "{key}");
            assert_eq!(partition_of("rabbitmq", key, 6), partition, "{key}");
        }
    }

    #[test]
    fn every_family_has_a_key_on_every_partition() {
        for family in FAMILIES {
            let keys = partition_keys(family, "run42");
            assert_eq!(partition_of(family, &keys[0], PARTITIONS), 0);
            assert_eq!(partition_of(family, &keys[1], PARTITIONS), 1);
        }
    }

    #[test]
    fn names_carry_the_run() {
        assert_eq!(
            source_name("run42", "postgres"),
            "snodra.interop.run42.postgres"
        );
        assert_eq!(database_name("run42"), "snodra_interop_run42");
        assert_eq!(instance("go", "mongo"), "go-mongo");
        assert_eq!(subscription_name("kafka"), "store-interop.interop-kafka");
    }
}
