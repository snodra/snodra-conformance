//! The hub choice of a keyed request (protocol.md, "Several hubs"): the rendezvous score.

/// `fmix64(fnv1a64(utf8(hub_id) ++ [0x00] ++ utf8(key)))`.
pub fn score(hub_id: &str, key: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in hub_id.bytes().chain(std::iter::once(0)).chain(key.bytes()) {
        h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut k = h;
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// The hubs in the order a keyed request tries them: highest score first, equal scores by the
/// smaller hub id (bytes).
pub fn ranking<'a>(hub_ids: &[&'a str], key: &str) -> Vec<&'a str> {
    let mut ids = hub_ids.to_vec();
    ids.sort_by(|a, b| {
        score(b, key)
            .cmp(&score(a, key))
            .then(a.as_bytes().cmp(b.as_bytes()))
    });
    ids
}

/// The ranking a library comparing the scores as signed numbers would make (wrong: the
/// runner uses it only to pick keys that tell the two apart).
pub fn ranking_signed<'a>(hub_ids: &[&'a str], key: &str) -> Vec<&'a str> {
    let mut ids = hub_ids.to_vec();
    ids.sort_by(|a, b| {
        (score(b, key) as i64)
            .cmp(&(score(a, key) as i64))
            .then(a.as_bytes().cmp(b.as_bytes()))
    });
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shared_test_vectors_hold() {
        assert_eq!(score("", ""), 0xb903_4ad3_7056_f5fb);
        assert_eq!(score("hub-1", "order-42"), 0x5c85_5509_f5ad_7c8d);
        assert_eq!(score("hub-2", "order-42"), 0xd109_ca17_7e04_ae14);
        assert_eq!(
            ranking(&["hub-1", "hub-2"], "order-42"),
            vec!["hub-2", "hub-1"]
        );
    }
}
