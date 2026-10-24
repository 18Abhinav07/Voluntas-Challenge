use super::*;
use crate::{
    adapter::{Instrument, LevelChange},
    book::Side,
};

#[derive(Default)]
struct Fx {
    spawned: Vec<(ConnId, usize, Vec<Sym>)>,
    sent: Vec<(ConnId, String)>,
    lookups: Vec<Sym>,
}

impl Effects for Fx {
    fn spawn_conn(&mut self, id: ConnId, replica: usize, tokens: Vec<Sym>) {
        self.spawned.push((id, replica, tokens));
    }
    fn send(&mut self, id: ConnId, cmd: ConnCmd) {
        self.sent.push((id, format!("{cmd:?}")));
    }
    fn lookup(&mut self, market: Sym) {
        self.lookups.push(market);
    }
}

struct NoCheck;
impl Integrity<()> for NoCheck {
    fn register(&mut self, _: &Instrument<()>) {}
    fn pre_event(&mut self, _: ConnId, _: &NormEvent, _: &dyn BookView, _: Instant, _: &mut Vec<Verdict>) {}
    fn on_event(&mut self, _: ConnId, _: &NormEvent, _: &dyn BookView, _: Instant, _: &mut Vec<Verdict>) {}
    fn on_tick(&mut self, _: Instant, _: &mut Vec<Verdict>) {}
    fn forget_conn(&mut self, _: ConnId) {}
}

fn inst(m: &str, toks: &[&str]) -> Instrument<()> {
    Instrument { market: m.into(), tokens: toks.iter().map(|&t| t.into()).collect(), meta: () }
}

fn book(t: &str, ts: Ts, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> NormEvent {
    NormEvent::Book {
        token: t.into(),
        market: "m".into(),
        bids: bids.to_vec(),
        asks: asks.to_vec(),
        exch_ts: ts,
        check: None,
        tick: None,
        last_trade: None,
    }
}

fn lvl(t: &str, ts: Ts, side: Side, px: i64, qty: i64) -> NormEvent {
    NormEvent::Levels {
        market: "m".into(),
        exch_ts: ts,
        changes: vec![LevelChange { token: t.into(), side, px, qty, check: None }],
    }
}

/// The updates in `out`, and how many snapshots follow them. A source switch writes its diff
/// and then a snapshot of both sides (D8).
fn diff_then_snap(out: &[Rec]) -> (Vec<Rec>, usize) {
    let n = out.iter().rev().take_while(|r| matches!(r, Rec::Snapshot { .. })).count();
    (out[..out.len() - n].to_vec(), n)
}

fn setup() -> (Core<()>, Fx, Instant) {
    let mut core = Core::new(Config { tokens_per_conn: 10, ..Config::default() }, Box::new(NoCheck));
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m", &["a", "b"])], &mut fx);
    core.flush_pending(now, &mut fx);
    (core, fx, now)
}

#[test]
fn initial_book_that_never_comes_is_asked_again() {
    let (mut core, mut fx, now) = setup();
    core.on_up(0, now);
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.retry_resnapshots(now + Duration::from_secs(60), &mut fx);
    assert_eq!(resnaps(&fx), 0, "dump still within its timeout");

    core.retry_resnapshots(now + core.cfg.book_timeout, &mut fx);
    assert_eq!(fx.sent, vec![(0, r#"Resnapshot(["b"])"#.to_string())], "only the missing book, one command");

    core.retry_resnapshots(now + core.cfg.book_timeout + Duration::from_secs(1), &mut fx);
    assert_eq!(resnaps(&fx), 1, "debounced by resnap_gap");
}

#[test]
fn socket_that_never_came_up_is_not_asked() {
    let (mut core, mut fx, now) = setup();
    core.retry_resnapshots(now + Duration::from_secs(600), &mut fx);
    assert_eq!(resnaps(&fx), 0, "a socket still waiting to open gets its dump on subscribe");

    core.on_up(1, now);
    core.on_down(1, 1);
    core.retry_resnapshots(now + Duration::from_secs(600), &mut fx);
    assert_eq!(resnaps(&fx), 0, "a dropped socket resubscribes everything when it is back");
}

#[test]
fn connection_cap_leaves_tokens_unplaced() {
    let mut core = Core::new(Config { tokens_per_conn: 2, max_conns: 2, ..Config::default() }, Box::new(NoCheck));
    let mut fx = Fx::default();
    core.add(vec![inst("m", &["a", "b"]), inst("n", &["c", "d"])], &mut fx);
    core.flush_pending(Instant::now(), &mut fx);
    assert_eq!((fx.spawned.len(), core.metrics.unplaced), (2, 2), "one shard × two replicas, then capped");
}

#[test]
fn plans_one_shard_with_two_replicas() {
    let (core, fx, _) = setup();
    assert_eq!(fx.spawned.len(), 2);
    assert_eq!(fx.spawned[0].2, fx.spawned[1].2);
    assert_eq!(core.conn_count(), 2);
}

#[test]
fn first_book_snapshots_then_only_source_emits() {
    let (mut core, mut fx, now) = setup();
    core.on_events(1, 10, &[book("a", 5, &[(400, 1)], &[(600, 2)])], now, &mut fx);
    assert!(matches!(&core.out[..], [Rec::Snapshot { exch: 5, .. }, Rec::Snapshot { .. }]));
    core.out.clear();
    core.on_events(0, 11, &[book("a", 5, &[(400, 1)], &[(600, 2)])], now, &mut fx);
    assert!(core.out.is_empty(), "non-source book is silent");
    core.on_events(0, 12, &[lvl("a", 6, Side::Bid, 410, 3)], now, &mut fx);
    assert!(core.out.is_empty(), "non-source level is silent");
    core.on_events(1, 13, &[lvl("a", 6, Side::Bid, 410, 3)], now, &mut fx);
    assert_eq!(core.out, vec![Rec::Update { sym: "a".into(), side: Side::Bid, recv: 13, exch: 6, px: 410, qty: 3 }]);
}

#[test]
fn disconnect_fails_over_with_snapshot_from_peer() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 2, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 3, &[lvl("a", 7, Side::Ask, 600, 9)], now, &mut fx);
    core.out.clear();
    core.on_down(0, 4);
    assert_eq!(
        diff_then_snap(&core.out),
        (vec![Rec::Update { sym: "a".into(), side: Side::Ask, recv: 4, exch: 7, px: 600, qty: 9 }], 2),
        "peer's extra level as a diff, then a snapshot"
    );
    assert!(matches!(&core.out[2], Rec::Snapshot { levels, .. } if levels == &[(600, 9)]));
    assert_eq!((core.metrics.switches, core.metrics.switch_snaps), (1, 1));
}

#[test]
fn no_valid_replica_goes_dark_until_book() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_down(0, 2);
    core.out.clear();
    core.on_events(0, 3, &[lvl("a", 6, Side::Bid, 400, 0)], now, &mut fx);
    assert!(core.out.is_empty(), "no deltas against an untrusted book");
    core.on_events(1, 4, &[book("a", 6, &[], &[])], now, &mut fx);
    assert_eq!(diff_then_snap(&core.out), (vec![Rec::Update { sym: "a".into(), side: Side::Bid, recv: 4, exch: 6, px: 400, qty: 0 }], 2));
}

#[test]
fn mid_stream_book_on_source_emits_diff() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1), (300, 1)], &[])], now, &mut fx);
    core.out.clear();
    core.on_events(0, 2, &[book("a", 9, &[(400, 2)], &[])], now, &mut fx);
    let mut got = core.out.clone();
    got.sort_by_key(|r| format!("{r:?}"));
    assert_eq!(
        got,
        vec![
            Rec::Update { sym: "a".into(), side: Side::Bid, recv: 2, exch: 9, px: 300, qty: 0 },
            Rec::Update { sym: "a".into(), side: Side::Bid, recv: 2, exch: 9, px: 400, qty: 2 },
        ]
    );
    assert_eq!(core.metrics.book_diffs, 1);
}

#[test]
fn lagging_source_switches_and_resnapshots() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
    core.out.clear();
    core.on_tick(now + Duration::from_secs(1), 3, &mut fx);
    assert_eq!(core.metrics.lag_switches, 0, "within the lag window");
    core.on_tick(now + Duration::from_secs(4), 4, &mut fx);
    assert_eq!(core.metrics.lag_switches, 1);
    assert!(fx.sent.iter().any(|(c, s)| *c == 0 && s.starts_with("Resnapshot")));
    assert_eq!(diff_then_snap(&core.out), (vec![Rec::Update { sym: "a".into(), side: Side::Bid, recv: 4, exch: 6, px: 400, qty: 2 }], 2));
}

#[test]
fn stalled_source_switches_while_the_peer_keeps_ticking() {
    // The peer's newest delta is always fresh; the lag runs from its oldest unseen one.
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    for i in 0..5 {
        let t = now + Duration::from_millis(900 * i);
        core.on_events(1, 2, &[lvl("a", 6 + i as i64, Side::Bid, 400, 2 + i as i64)], t, &mut fx);
        core.on_tick(t, 3, &mut fx);
    }
    assert_eq!(core.metrics.lag_switches, 1);
}

#[test]
fn source_racing_a_peer_is_not_lag() {
    // Each peer delta reaches the source 100 ms later: never behind by the lag window.
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    for i in 0..50 {
        let t = now + Duration::from_millis(200 * i);
        let e = lvl("a", 6 + i as i64, Side::Bid, 400, 2 + i as i64);
        core.on_events(1, 2, std::slice::from_ref(&e), t, &mut fx);
        core.on_tick(t + Duration::from_millis(50), 3, &mut fx);
        core.on_events(0, 2, &[e], t + Duration::from_millis(100), &mut fx);
    }
    assert_eq!(core.metrics.lag_switches, 0);
}

/// Both replicas hold the same book at ts 5; the source (conn 0) then sees one more delta.
fn ahead_by_one() -> (Core<()>, Fx, Instant) {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 410, 1)], now, &mut fx);
    (core, fx, now)
}

fn miss(core: &mut Core<()>, fx: &mut Fx, now: Instant, conn: ConnId) {
    core.verdicts.push(Verdict::Miss { conn, token: "a".into(), exch_ts: 6 });
    core.handle_verdicts(3, now, fx);
}

fn resnaps(fx: &Fx) -> usize {
    fx.sent.iter().filter(|(_, s)| s.starts_with("Resnapshot")).count()
}

#[test]
fn single_uncorroborated_miss_is_tolerated() {
    let (mut core, mut fx, now) = ahead_by_one();
    miss(&mut core, &mut fx, now, 0);
    assert_eq!((core.metrics.hash_miss, resnaps(&fx), core.metrics.switches), (1, 0, 0));
    miss(&mut core, &mut fx, now, 0);
    assert_eq!(resnaps(&fx), 1, "second strike refreshes");
    assert_eq!(core.metrics.switches, 1, "source moves off before the gap opens");
}

#[test]
fn verified_match_clears_strikes() {
    let (mut core, mut fx, now) = ahead_by_one();
    miss(&mut core, &mut fx, now, 0);
    core.verdicts.push(Verdict::Ok { conn: 0, token: "a".into(), exch_ts: 6 });
    miss(&mut core, &mut fx, now, 0);
    assert_eq!(resnaps(&fx), 0);
}

#[test]
fn miss_corroborated_by_peer_refreshes_at_once() {
    let (mut core, mut fx, now) = ahead_by_one();
    core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 420, 1)], now, &mut fx);
    miss(&mut core, &mut fx, now, 0);
    assert_eq!((core.metrics.divergent, resnaps(&fx)), (1, 1));
}

#[test]
fn miss_with_agreeing_peer_counts_as_agreed() {
    let (mut core, mut fx, now) = ahead_by_one();
    core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 410, 1)], now, &mut fx);
    miss(&mut core, &mut fx, now, 0);
    assert_eq!((core.metrics.hash_agreed, core.metrics.divergent, resnaps(&fx)), (1, 0, 0));
}

#[test]
fn only_the_source_is_offered_for_hashing() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    assert!(View { tokens: &core.tokens, conn: 0 }.book("a").is_some());
    assert!(View { tokens: &core.tokens, conn: 1 }.book("a").is_none(), "standby is not hashed");
}

#[test]
fn caught_up_source_leaves_the_lag_candidates() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 2, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
    assert_eq!(core.lag_cands.len(), 1);
    core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
    core.on_tick(now + Duration::from_secs(1), 3, &mut fx);
    assert!(core.lag_cands.is_empty() && !core.tokens["a"].lag_cand);
    core.on_tick(now + Duration::from_secs(10), 4, &mut fx);
    assert_eq!(core.metrics.lag_switches, 0);
}

#[test]
fn fresh_peer_book_is_not_lag() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 2, &[book("a", 50, &[(400, 1)], &[])], now, &mut fx);
    core.on_tick(now + Duration::from_secs(10), 3, &mut fx);
    assert_eq!(core.metrics.lag_switches, 0, "a dump's timestamp is not evidence of missed deltas");
}

#[test]
fn socket_closed_as_all_resolved_ends_its_markets() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[(600, 1)])], now, &mut fx);
    core.out.clear();
    // No market_resolved event: the venue just closes the socket.
    core.on_resolved(0, &["a".into(), "b".into()], 2, &mut fx);
    assert_eq!(core.metrics.resolved_closes, 1);
    assert_eq!(core.out.len(), 2);
    assert!(core.out.iter().all(|r| matches!(r, Rec::Update { qty: 0, .. })));
    assert_eq!(core.token_count(), 0);
    assert_eq!(fx.sent.iter().filter(|(_, s)| s.starts_with("Unsubscribe")).count(), 2);
}

#[test]
fn resolved_market_zeroes_levels_and_unsubscribes() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[(600, 1)])], now, &mut fx);
    core.out.clear();
    core.on_events(0, 2, &[NormEvent::Resolved { market: "m".into() }], now, &mut fx);
    assert_eq!(core.out.len(), 2);
    assert!(core.out.iter().all(|r| matches!(r, Rec::Update { qty: 0, .. })));
    assert_eq!(core.token_count(), 0);
    assert_eq!(fx.sent.iter().filter(|(_, s)| s.starts_with("Unsubscribe")).count(), 2);
}

#[test]
fn listed_market_is_looked_up_once() {
    let (mut core, mut fx, now) = setup();
    let ev = NormEvent::Listed { market: "n".into() };
    core.on_events(0, 1, std::slice::from_ref(&ev), now, &mut fx);
    core.on_events(1, 1, std::slice::from_ref(&ev), now, &mut fx);
    assert_eq!(fx.lookups.len(), 1);
    core.add(vec![inst("n", &["c", "d"])], &mut fx);
    assert_eq!(fx.sent.iter().filter(|(_, s)| s.starts_with("Subscribe")).count(), 2, "joins the tail shard");
}

#[test]
fn snapshots_every_period_per_token() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(0, 1, &[book("b", 5, &[(400, 1)], &[])], now, &mut fx);
    core.out.clear();
    for s in 0..60 {
        core.on_tick(now + Duration::from_secs(s), s as i64, &mut fx);
    }
    assert_eq!(core.out.iter().filter(|r| matches!(r, Rec::Snapshot { .. })).count(), 4);
}

#[test]
fn rotation_marks_the_cut_and_snapshots_every_published_book() {
    let (mut core, mut fx, now) = setup();
    core.cfg.rotate_every = Some(Duration::from_secs(10));
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(0, 1, &[book("b", 5, &[(400, 1)], &[])], now, &mut fx);
    let cut = |out: &[Rec]| out.iter().position(|r| matches!(r, Rec::Rotate { .. }));
    let snaps = |out: &[Rec], t: &str| out.iter().filter(|r| matches!(r, Rec::Snapshot { sym, .. } if sym.starts_with(t))).count();
    core.out.clear();
    core.on_tick(now, 3_000_000, &mut fx);
    assert_eq!(core.out[0], Rec::Rotate { epoch: 0 });
    assert_eq!(snaps(&core.out, ""), 2, "spread over half the period: one book per tick");
    core.on_tick(now + Duration::from_secs(1), 9_999_999, &mut fx);
    assert_eq!(cut(&core.out[1..]), None, "same period");
    assert_eq!(snaps(&core.out, "a"), 2);
    assert_eq!(snaps(&core.out, "b"), 2);
    core.out.clear();
    core.on_tick(now + Duration::from_secs(2), 10_000_000, &mut fx);
    assert_eq!(core.out[0], Rec::Rotate { epoch: 1 });
    for s in 3..8 {
        core.on_tick(now + Duration::from_secs(s), 10_000_000 + s as i64, &mut fx);
    }
    assert!(snaps(&core.out, "a") >= 2 && snaps(&core.out, "b") >= 2, "every book again in the new period");
}

#[test]
fn update_before_its_rotation_turn_snapshots_first() {
    let (mut core, mut fx, now) = setup();
    core.cfg.rotate_every = Some(Duration::from_secs(600));
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(0, 1, &[book("b", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_tick(now, 3_000_000, &mut fx);
    core.on_tick(now, 600_000_000, &mut fx);
    core.out.clear();
    core.on_events(0, 600_000_001, &[lvl("a", 6, Side::Bid, 400, 2)], now, &mut fx);
    core.on_events(0, 600_000_001, &[lvl("b", 6, Side::Bid, 400, 3)], now, &mut fx);
    let kinds: Vec<&str> = core.out.iter().map(|r| match r { Rec::Snapshot { .. } => "S", Rec::Update { .. } => "U", _ => "?" }).collect();
    // One of a/b was drained at the cut; the other is snapshotted right before its update.
    assert!(kinds == ["U", "S", "S", "U"] || kinds == ["S", "S", "U", "U"], "{kinds:?}");
    assert_eq!(core.metrics.rot_early, 1);
    for s in 1..12 {
        core.on_tick(now + Duration::from_secs(s), 600_000_000 + s as i64, &mut fx);
    }
    assert_eq!(core.metrics.rot_early, 1, "the drain skips books already written this period");
}

#[test]
fn partial_shard_opens_after_wait() {
    let (mut core, mut fx, now) = setup();
    core.add(vec![inst("x", &["p", "q", "r", "s", "t", "u", "v", "w", "y"])], &mut fx);
    assert_eq!(fx.spawned.len(), 2, "tail shard full: waits for more tokens");
    core.on_tick(now + PENDING_MAX_WAIT * 2, 1, &mut fx);
    assert_eq!(fx.spawned.len(), 4);
}

#[test]
fn recycle_is_make_before_break() {
    let mut core = Core::new(
        Config { tokens_per_conn: 10, max_conn_age: Some(Duration::from_secs(10)), ..Config::default() },
        Box::new(NoCheck),
    );
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m", &["a"])], &mut fx);
    core.flush_pending(now, &mut fx);
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_tick(now + Duration::from_secs(11), 2, &mut fx);
    assert_eq!(fx.spawned.len(), 3, "new connection opened first");
    assert!(!fx.sent.iter().any(|(_, s)| s == "Close"), "old one still open");
    core.out.clear();
    core.on_events(2, 3, &[book("a", 6, &[(400, 1)], &[])], now, &mut fx);
    assert!(fx.sent.iter().any(|(c, s)| *c == 0 && s == "Close"));
    assert_eq!(core.metrics.recycles, 1);
    assert_eq!(diff_then_snap(&core.out), (vec![], 2), "identical books: the handover writes only a snapshot");
}

fn set_rate(core: &mut Core<()>, t: &str, rate: f32) {
    core.tokens.get_mut(t).unwrap().rate = rate;
}

#[test]
fn quiet_token_is_resubscribed_on_every_replica() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    core.on_events(1, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    // 6 deltas a minute: one every 10 s, so quiet after 20 gaps = 200 s.
    set_rate(&mut core, "a", 6.0);
    core.check_quiet(now + Duration::from_secs(199), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 0, "within its usual silence");
    core.check_quiet(now + Duration::from_secs(201), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 1);
    let mut to: Vec<ConnId> = fx.sent.iter().filter(|(_, s)| s == r#"Resnapshot(["a"])"#).map(|(c, _)| *c).collect();
    to.sort();
    assert_eq!(to, vec![0, 1], "every replica resubscribes: none of them is hearing anything");
}

#[test]
fn quiet_backs_off_through_deltas_until_a_resubscribe_catches_a_miss() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    set_rate(&mut core, "a", 6.0);
    let at = |s| now + Duration::from_secs(s);
    core.check_quiet(at(201), &mut fx);
    core.check_quiet(at(500), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 1, "second wait is doubled to 400 s");
    core.check_quiet(at(602), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 2);
    // A burst after the silence is how bursty markets look; it is no proof of a miss.
    core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 410, 1)], at(610), &mut fx);
    assert_eq!(core.tokens["a"].quiet_strikes, 2);
    // The answering book matches what readers hold: a clean probe, strikes stay.
    core.on_events(0, 3, &[book("a", 7, &[(400, 1), (410, 1)], &[])], at(611), &mut fx);
    assert_eq!((core.tokens["a"].quiet_strikes, core.metrics.quiet_caught), (2, 0));
    core.check_quiet(at(1409), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 2, "800 s wait from the last delta");
    core.check_quiet(at(1411), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 3);
    // This time the book shows a level we never heard about: a real miss.
    core.on_events(0, 4, &[book("a", 8, &[(400, 1), (410, 1), (420, 3)], &[])], at(1413), &mut fx);
    assert_eq!((core.tokens["a"].quiet_strikes, core.metrics.quiet_caught), (0, 1));
}

#[test]
fn strikes_settle_while_active_long_after_the_last_resubscribe() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    let tok = core.tokens.get_mut("a").unwrap();
    (tok.quiet_strikes, tok.quiet_at) = (3, Some(now));
    core.on_events(0, 2, &[lvl("a", 6, Side::Bid, 410, 1)], now, &mut fx);
    core.roll_rates(now + Duration::from_secs(60));
    assert_eq!(core.tokens["a"].quiet_strikes, 3, "too soon after the resubscribe");
    core.on_events(0, 2, &[lvl("a", 7, Side::Bid, 410, 2)], now, &mut fx);
    core.roll_rates(now + core.cfg.quiet.ceiling);
    assert_eq!(core.tokens["a"].quiet_strikes, 2);
    core.roll_rates(now + core.cfg.quiet.ceiling * 2);
    assert_eq!(core.tokens["a"].quiet_strikes, 2, "only while deltas flow");
}

#[test]
fn capped_round_takes_the_most_overdue() {
    let (mut core, mut fx, now) = setup();
    core.cfg.quiet.max_per_round = 1;
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[]), book("b", 5, &[(400, 1)], &[])], now, &mut fx);
    set_rate(&mut core, "a", 6.0);
    set_rate(&mut core, "b", 60.0);
    core.check_quiet(now + Duration::from_secs(300), &mut fx);
    assert!(fx.sent.iter().any(|(_, s)| s == r#"Resnapshot(["b"])"#), "b is 5x overdue, a 1.5x");
    assert_eq!(core.metrics.quiet_resubs, 1);
}

#[test]
fn slow_token_is_left_to_recycling() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    set_rate(&mut core, "a", 0.5);
    core.check_quiet(now + Duration::from_secs(3 * 3600), &mut fx);
    assert_eq!(core.metrics.quiet_resubs, 0, "below min_rate silence says nothing");
}

#[test]
fn rates_follow_delta_counts() {
    let (mut core, mut fx, now) = setup();
    core.on_events(0, 1, &[book("a", 5, &[(400, 1)], &[])], now, &mut fx);
    for ts in 6..16 {
        core.on_events(0, 2, &[lvl("a", ts, Side::Bid, 400, ts)], now, &mut fx);
        core.on_events(1, 2, &[lvl("a", ts, Side::Bid, 400, ts)], now, &mut fx);
    }
    core.roll_rates(now);
    assert_eq!(core.tokens["a"].rate, 1.0, "10 distinct deltas (seen on two sockets) smoothed by 0.1");
    assert_eq!(core.tokens["b"].rate, 0.0);
}

fn busy_window(core: &mut Core<()>, fx: &mut Fx, now: Instant, conn: ConnId, n: i64) {
    for ts in 0..n {
        core.on_events(conn, 2, &[lvl("a", 100 + ts, Side::Bid, 400, 1)], now, fx);
    }
}

#[test]
fn socket_quiet_while_peer_is_busy_reconnects() {
    let (mut core, mut fx, now) = setup();
    core.on_up(0, now);
    core.on_up(1, now);
    busy_window(&mut core, &mut fx, now, 1, 40);
    core.check_quiet_conns(now + Duration::from_secs(31), &mut fx);
    assert_eq!(fx.sent, vec![(0, "Reconnect".to_string())]);
    assert_eq!(core.metrics.quiet_reconnects, 1);
    core.check_quiet_conns(now + Duration::from_secs(62), &mut fx);
    assert_eq!(core.metrics.quiet_reconnects, 1, "not again until it is back up for a window");
}

#[test]
fn shard_quiet_on_both_sockets_is_not_a_socket_fault() {
    let (mut core, mut fx, now) = setup();
    core.on_up(0, now);
    core.on_up(1, now);
    busy_window(&mut core, &mut fx, now, 0, 5);
    core.check_quiet_conns(now + Duration::from_secs(31), &mut fx);
    assert_eq!(core.metrics.quiet_reconnects, 0, "a peer below conn_min is no evidence");
    core.check_quiet_conns(now + Duration::from_secs(10), &mut fx);
    assert_eq!(core.metrics.quiet_reconnects, 0);
}

#[test]
fn recycles_run_in_parallel_across_shards_only() {
    let mut core = Core::new(
        Config { tokens_per_conn: 1, max_conn_age: Some(Duration::from_secs(10)), recycle_parallel: 2, ..Config::default() },
        Box::new(NoCheck),
    );
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m", &["a"]), inst("n", &["b"]), inst("o", &["c"])], &mut fx);
    core.flush_pending(now, &mut fx);
    assert_eq!(fx.spawned.len(), 6, "three shards × two replicas");
    core.maybe_start_recycle(now + Duration::from_secs(11), &mut fx);
    let new: Vec<_> = fx.spawned[6..].iter().map(|(id, _, t)| (*id, t.clone())).collect();
    assert_eq!(new.len(), 2, "capped at recycle_parallel");
    assert_ne!(new[0].1, new[1].1, "never both replicas of one shard");
    core.maybe_start_recycle(now + Duration::from_secs(12), &mut fx);
    assert_eq!(fx.spawned.len(), 8, "no more until one finishes");
}

/// Three shards of two replicas: [a..f] on 0/1, [g..h3] on 2/3, [i j] on 4/5, all up with
/// books on both replicas and 10 deltas a minute per token.
fn three_shards() -> (Core<()>, Fx, Instant) {
    let cfg = Config { tokens_per_conn: 6, split_ratio: Some(3.0), split_min_rate: 100.0, ..Config::default() };
    let mut core = Core::new(cfg, Box::new(NoCheck));
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m1", &["a", "b"]), inst("m2", &["c", "d"]), inst("m3", &["e", "f"])], &mut fx);
    core.add(vec![inst("m4", &["g", "h"]), inst("m7", &["g2", "h2"]), inst("m8", &["g3", "h3"])], &mut fx);
    core.add(vec![inst("m5", &["i", "j"])], &mut fx);
    core.flush_pending(now, &mut fx);
    for c in 0..6 {
        core.on_up(c, now);
        for t in &core.shards[c as usize / 2].tokens.clone() {
            core.on_events(c, 1, &[book(t, 5, &[(400, 1)], &[])], now, &mut fx);
        }
    }
    for t in core.tokens.keys().cloned().collect::<Vec<_>>() {
        set_rate(&mut core, &t, 10.0);
    }
    (core, fx, now)
}

#[test]
fn hot_shard_splits_make_before_break() {
    let (mut core, mut fx, now) = three_shards();
    set_rate(&mut core, "a", 1000.0);
    set_rate(&mut core, "b", 1000.0);
    core.maybe_split(now, &mut fx);
    assert_eq!(core.metrics.splits, 1);
    let new: Vec<_> = fx.spawned[6..].iter().map(|(c, _, ts)| (*c, ts.clone())).collect();
    assert_eq!(new, vec![(6, vec![Sym::from("a"), "b".into()]), (7, vec!["a".into(), "b".into()])], "only the hot market moves");
    assert!(!fx.sent.iter().any(|(_, s)| s.starts_with("Unsubscribe")), "old sockets keep serving meanwhile");
    assert_eq!(core.tokens["a"].source, Some(0));
    core.out.clear();
    for c in [6, 7] {
        core.on_up(c, now);
        core.on_events(c, 2, &[book("a", 6, &[(400, 1)], &[]), book("b", 6, &[(400, 1)], &[])], now, &mut fx);
    }
    let unsub: Vec<_> = fx.sent.iter().filter(|(_, s)| s.starts_with("Unsubscribe")).cloned().collect();
    assert_eq!(unsub, vec![(0, r#"Unsubscribe(["a", "b"])"#.to_string()), (1, r#"Unsubscribe(["a", "b"])"#.to_string())]);
    assert!(core.tokens["a"].source.is_some_and(|s| s >= 6), "source handed to the new socket");
    assert_eq!(core.tokens["a"].replicas.len(), 2);
    assert_eq!(core.shards[0].tokens.len(), 4);
    assert_eq!(diff_then_snap(&core.out), (vec![], 4), "identical books: the split writes only snapshots");
    // The old socket dropping no longer touches the moved tokens.
    core.on_down(0, 3);
    assert!(core.tokens["a"].source.is_some_and(|s| s >= 6));
}

#[test]
fn shard_near_the_median_does_not_split() {
    let (mut core, mut fx, now) = three_shards();
    set_rate(&mut core, "a", 25.0);
    core.maybe_split(now, &mut fx);
    assert_eq!(core.metrics.splits, 0, "75 a minute against a median of 60: under 3x");
    core.cfg.split_min_rate = 10_000.0;
    set_rate(&mut core, "a", 1000.0);
    core.maybe_split(now, &mut fx);
    assert_eq!(core.metrics.splits, 0, "under the absolute floor");
}

#[test]
fn split_hands_over_after_grace_and_survives_a_resolved_market() {
    let (mut core, mut fx, now) = three_shards();
    set_rate(&mut core, "a", 1000.0);
    set_rate(&mut core, "c", 900.0);
    core.maybe_split(now, &mut fx);
    assert_eq!(core.split.as_ref().unwrap().moved.len(), 2, "m1 alone is half the rate");
    core.remove_market("m1", 2, &mut fx);
    assert_eq!(core.shards[0].tokens.len(), 4, "gone from both shards");
    core.on_tick(now + Duration::from_secs(61), 3, &mut fx);
    assert!(core.split.is_none(), "grace ran out");
    assert_eq!(core.shards[0].tokens.len(), 4);
}

#[test]
fn late_markets_do_not_join_a_split_off_shard() {
    let (mut core, mut fx, now) = three_shards();
    set_rate(&mut core, "a", 1000.0);
    set_rate(&mut core, "b", 1000.0);
    core.maybe_split(now, &mut fx);
    core.add(vec![inst("m6", &["k", "l"])], &mut fx);
    assert_eq!(core.tokens["k"].shard, 2, "joins the last ordinary shard");
}

#[test]
fn conn_ids_start_at_the_core_base() {
    let mut core = Core::new(Config { tokens_per_conn: 10, conn_id_base: 1 << 20, ..Config::default() }, Box::new(NoCheck));
    let mut fx = Fx::default();
    core.add(vec![inst("m", &["a"])], &mut fx);
    core.flush_pending(Instant::now(), &mut fx);
    assert_eq!(fx.spawned.iter().map(|s| s.0).collect::<Vec<_>>(), vec![1 << 20, (1 << 20) + 1]);
}

#[test]
fn ms_hist_keeps_negative_wire_and_exact_mean() {
    let mut h = MsHist::default();
    for us in [-20_000, 3_400, 3_900, 9_000_000] {
        h.record(us);
    }
    assert_eq!((h.n, h.quantile_ms(0.25), h.quantile_ms(0.5), h.quantile_ms(1.0)), (4, -20, 3, 1535), "a 9 s outlier lands in the top bucket");
    assert!((h.mean_ms() - 2_246.825).abs() < 1e-9, "the mean is not clamped");
}

#[test]
fn behind_is_timed_against_the_first_socket_to_deliver_an_event() {
    let mut core = Core::new(Config { tokens_per_conn: 10, lat_detail: true, ..Config::default() }, Box::new(NoCheck));
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m", &["a", "b"])], &mut fx);
    core.flush_pending(now, &mut fx);
    let ev = |ts| [lvl("a", ts, Side::Bid, 400, 1)];
    core.on_events(1, 10_000, &ev(5_000), now, &mut fx);
    core.on_events(1, 10_100, &ev(5_000), now, &mut fx);
    core.on_events(0, 13_000, &ev(5_000), now, &mut fx);
    core.on_events(0, 20_000, &ev(9_000), now, &mut fx);
    core.on_events(1, 20_500, &ev(9_000), now, &mut fx);
    for ts in 10..30 {
        core.on_events(0, 30_000 + ts * 1_000, &ev(ts * 1_000), now, &mut fx);
    }
    core.on_events(1, 99_000, &ev(5_000), now, &mut fx);

    let (r0, r1) = (&core.lat[&0], &core.lat[&1]);
    assert_eq!((r1.first, r1.behind.n, r1.late), (1, 2, 1), "second same-ms event untimed; a copy past the window is late");
    assert_eq!((r0.first, r0.behind.n), (21, 22));
    assert_eq!(r0.behind.quantile_ms(0.0), 0);
    assert!((r0.behind.mean_ms() - 3.0 / 22.0).abs() < 1e-9, "3 ms behind once, first otherwise");
    assert!((r1.behind.mean_ms() - 0.25).abs() < 1e-9, "first once, 0.5 ms behind once");
    assert_eq!(r1.wire.n, 4, "wire counts every event, timed or not");
}

/// One shard [a b] on sockets 0 and 1, both up with identical books, 0 the source of both.
fn fast_pair(f: Fast) -> (Core<()>, Fx, Instant) {
    let mut core = Core::new(Config { tokens_per_conn: 10, fast: Some(f), ..Config::default() }, Box::new(NoCheck));
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m", &["a", "b"])], &mut fx);
    core.flush_pending(now, &mut fx);
    for c in [0, 1] {
        core.on_up(c, now);
        core.on_events(c, 1, &[book("a", 1, &[(400, 1)], &[]), book("b", 1, &[(300, 1)], &[])], now, &mut fx);
    }
    (core, fx, now)
}

/// 60 events on "a", each delivered by `fast` first and by the other socket `gap_us` later.
fn race(core: &mut Core<()>, fx: &mut Fx, now: Instant, fast: ConnId, gap_us: i64, from: i64) {
    for i in from..from + 60 {
        let ev = [lvl("a", i * 10_000, Side::Bid, 400, i)];
        core.on_events(fast, i * 10_000 + 1_000, &ev, now, fx);
        core.on_events(1 - fast, i * 10_000 + 1_000 + gap_us, &ev, now, fx);
    }
}

#[test]
fn sources_follow_the_fastest_socket_past_a_margin() {
    let (mut core, mut fx, now) = fast_pair(Fast { margin_ms: 1.0, weight: 0.5, ..Fast::default() });
    assert_eq!(core.tokens["a"].source, Some(0));
    race(&mut core, &mut fx, now, 1, 5_000, 1);
    assert_eq!((core.metrics.src_behind.n, core.metrics.src_behind.mean_ms()), (60, 5.0), "written events came 5 ms late");
    core.out.clear();
    core.check_speed(now, 2, &mut fx);
    assert_eq!((core.tokens["a"].source, core.tokens["b"].source), (Some(1), Some(1)));
    assert_eq!((core.metrics.fast_picks, core.metrics.fast_switches), (1, 2));
    assert_eq!(diff_then_snap(&core.out), (vec![], 4), "equal books: the move writes only snapshots");

    // A fast socket whose book differs from the source's is not trusted for speed.
    core.on_events(0, 5, &[book("b", 2, &[(300, 9)], &[])], now, &mut fx);

    // Socket 0 now wins by 0.5 ms: within the margin, so the sources stay.
    race(&mut core, &mut fx, now, 0, 500, 100);
    core.check_speed(now, 3, &mut fx);
    assert_eq!((core.tokens["a"].source, core.metrics.fast_picks), (Some(1), 1));
    // By 10 ms: the scores cross by more than the margin, and the sources move back.
    race(&mut core, &mut fx, now, 0, 10_000, 200);
    core.check_speed(now, 4, &mut fx);
    assert_eq!((core.tokens["a"].source, core.metrics.fast_picks), (Some(0), 2));
    assert_eq!((core.tokens["b"].source, core.metrics.fast_disagree), (Some(1), 1), "books differ: b stays");
}

#[test]
fn failover_prefers_the_fast_socket_among_equals() {
    let cfg = Config { tokens_per_conn: 10, replicas: 3, fast: Some(Fast::default()), ..Config::default() };
    let mut core = Core::new(cfg, Box::new(NoCheck));
    let mut fx = Fx::default();
    let now = Instant::now();
    core.add(vec![inst("m", &["a"])], &mut fx);
    core.flush_pending(now, &mut fx);
    for c in [0, 1, 2] {
        core.on_events(c, 1, &[book("a", 1, &[(400, 1)], &[])], now, &mut fx);
    }
    core.shards[0].fast = Some(2);
    core.on_down(0, 5);
    assert_eq!(core.tokens["a"].source, Some(2), "1 and 2 are level; 2 is preferred");
}

#[test]
fn socket_that_stays_slowest_is_recycled() {
    let (mut core, mut fx, now) = fast_pair(Fast { slow_windows: 2, weight: 0.5, ..Fast::default() });
    race(&mut core, &mut fx, now, 1, 10_000, 1);
    core.check_speed(now, 2, &mut fx);
    assert_eq!(core.metrics.slow_recycles, 0, "one slow window is not enough");
    race(&mut core, &mut fx, now, 1, 10_000, 100);
    core.check_speed(now, 3, &mut fx);
    assert_eq!(core.metrics.slow_recycles, 1);
    let (id, replica, _) = fx.spawned.last().unwrap();
    assert_eq!((*id, *replica), (2, 0), "a new socket for replica 0, opened before 0 closes");
    assert!(core.conns[&0].retiring && !fx.sent.iter().any(|(_, s)| s == "Close"));
    race(&mut core, &mut fx, now, 1, 10_000, 200);
    core.check_speed(now, 4, &mut fx);
    assert_eq!(core.metrics.slow_recycles, 1, "a retiring socket is not scored");
}

#[test]
fn metrics_merge_sums_counts_and_histograms() {
    let mut a = Metrics { updates: 2, quiet_resubs: 1, ..Metrics::default() };
    a.wire.record(1000);
    let mut b = Metrics { updates: 3, ..Metrics::default() };
    b.wire.record(5000);
    a.merge(&b);
    assert_eq!((a.updates, a.quiet_resubs, a.wire.n, a.wire.max), (5, 1, 2, 5000));
}
