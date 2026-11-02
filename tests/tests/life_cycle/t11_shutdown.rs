use std::sync::Arc;

use anyhow::Result;
use maplit::btreeset;
use openraft::Config;
use openraft::Raft;
use openraft::ServerState;
use openraft::async_runtime::WatchReceiver;
use openraft::errors::Fatal;
use openraft::raft::TaskJoinOutcome;

use crate::fixtures::RaftRouter;
use crate::fixtures::ut_harness;

/// Shutdown raft node and check the metrics change.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn shutdown() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());
    let _log_index = router.new_cluster(btreeset! {0,1,2}, btreeset! {}).await?;

    tracing::info!("--- performing node shutdowns");
    {
        for i in [0, 1, 2] {
            let (node, _, _) = router.remove_node(i).unwrap();
            node.shutdown().await?;
            let m = node.metrics();
            assert_eq!(ServerState::Shutdown, m.borrow_watched().state, "shutdown node-{}", i);
        }
    }

    Ok(())
}

/// A panicked RaftCore should also return a proper error the next time accessing the `Raft`.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn return_error_after_panic() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing cluster");
    let log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;
    let _ = log_index; // unused;

    tracing::info!(log_index, "--- panic the RaftCore");
    {
        router
            .external_request(0, |_s| {
                panic!("foo");
            })
            .await?;
    }

    tracing::info!(
        log_index,
        "--- calls the panicked raft should get a Fatal::Panicked error"
    );
    {
        let res = router.client_request(0, "foo", 2).await;
        let err = res.unwrap_err();
        assert_eq!(Fatal::Panicked, err.into_fatal().unwrap());
    }

    Ok(())
}

/// A state-machine worker that dies while RaftCore stays alive must not hang callers.
///
/// `get_snapshot()` sends a command straight to the state-machine worker's channel, bypassing
/// RaftCore. When the worker has panicked, resolving the stop cause must fall back to
/// `Fatal::Stopped` within the bounded wait, instead of joining the still-running core forever.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn get_snapshot_returns_stopped_when_sm_worker_dies() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing single-node cluster");
    let _log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    tracing::info!("--- arm the state machine to panic on the next apply");
    {
        let (_log_store, sm) = router.get_storage_handle(&0)?;
        sm.panic_on_apply(true);
    }

    tracing::info!("--- a client write makes the SM worker panic and die; RaftCore stays up");
    {
        let res = router.client_request(0, "c", 1).await;
        assert!(
            res.is_err(),
            "write must fail: the SM worker panicked while applying it"
        );
    }

    tracing::info!("--- get_snapshot() must resolve to Fatal::Stopped, not hang");
    {
        let raft = router.get_raft_handle(&0)?;
        let err = raft.get_snapshot().await.unwrap_err();
        assert_eq!(Fatal::Stopped, err.into_fatal().unwrap());
    }

    Ok(())
}

/// `with_state_machine` must not hang when its function panics the state-machine worker.
///
/// The user function runs in the SM worker; a panic there kills the worker and drops the
/// responder while RaftCore keeps running. Resolving the stop cause must fall back to
/// `Fatal::Stopped` within the bounded wait instead of joining the still-running core forever.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn with_state_machine_returns_stopped_when_func_panics() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing single-node cluster");
    let _log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;

    tracing::info!("--- a panicking with_state_machine func kills the worker; the call must return Fatal::Stopped");
    {
        let raft = router.get_raft_handle(&0)?;
        let res = raft
            .with_state_machine::<_, ()>(|_sm| Box::pin(async move { panic!("injected worker panic") }))
            .await;
        assert_eq!(Fatal::Stopped, res.unwrap_err());
    }

    Ok(())
}

/// After shutdown(), access to Raft should return a Fatal::Stopped error.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn return_error_after_shutdown() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_heartbeat: false,
            ..Default::default()
        }
        .validate()?,
    );

    let mut router = RaftRouter::new(config.clone());

    tracing::info!("--- initializing cluster");
    let log_index = router.new_cluster(btreeset! {0}, btreeset! {}).await?;
    let _ = log_index; // unused;

    tracing::info!(log_index, "--- shutdown the raft");
    {
        let n = router.get_raft_handle(&0)?;
        n.shutdown().await?;
    }

    tracing::info!(
        log_index,
        "--- calls the panicked raft should get a Fatal::Panicked error"
    );
    {
        let res = router.client_request(0, "foo", 2).await;
        let err = res.unwrap_err();
        assert_eq!(Fatal::Stopped, err.into_fatal().unwrap());
    }

    Ok(())
}

/// Shutdown reports retain both joins, including across the compatibility API.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn shutdown_with_report() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config);

    tracing::info!(
        node = 0,
        "join the running core and tick tasks without starting an election"
    );
    let node = {
        router.new_raft_node(0).await;
        let node = router.get_raft_handle(&0)?;
        let report = node.shutdown_with_report().await;
        assert_eq!(TaskJoinOutcome::Joined, report.core);
        assert_eq!(TaskJoinOutcome::Joined, report.tick);
        assert!(!report.core_already_joined);
        assert!(!report.tick_already_joined);
        node
    };

    tracing::info!(
        node = 0,
        "a repeated shutdown reports cached outcomes and keeps the old API safe"
    );
    {
        node.shutdown().await?;
        let repeated = node.shutdown_with_report().await;
        assert_eq!(TaskJoinOutcome::Joined, repeated.core);
        assert_eq!(TaskJoinOutcome::Joined, repeated.tick);
        assert!(repeated.core_already_joined);
        assert!(repeated.tick_already_joined);
    }

    tracing::info!(
        node = 1,
        "the compatibility API also retains outcomes for later inspection"
    );
    {
        router.new_raft_node(1).await;
        let node = router.get_raft_handle(&1)?;
        node.shutdown().await?;
        let report = node.shutdown_with_report().await;
        assert_eq!(TaskJoinOutcome::Joined, report.core);
        assert_eq!(TaskJoinOutcome::Joined, report.tick);
        assert!(report.core_already_joined);
        assert!(report.tick_already_joined);
    }
    Ok(())
}

/// A built node reports unstarted tasks, including after repeated shutdown and run calls.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn shutdown_with_report_before_run() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config.clone());

    tracing::info!(node = 0, "build a node with fresh stores without spawning its tasks");
    let node = {
        let (log_store, sm) = router.new_store();
        Raft::build(0, config, router, log_store, sm).await?
    };

    tracing::info!(node = 0, "shutdown before run must never claim a successful task join");
    {
        for _ in 0..2 {
            let report = node.shutdown_with_report().await;
            assert_eq!(TaskJoinOutcome::NotStarted, report.core);
            assert_eq!(TaskJoinOutcome::NotStarted, report.tick);
            assert!(!report.core_already_joined);
            assert!(!report.tick_already_joined);
            node.run();
        }
        node.shutdown().await?;
    }
    Ok(())
}

/// An actual RaftCore panic remains observable after an API call has joined it.
#[tracing::instrument]
#[test_harness::test(harness = ut_harness)]
async fn shutdown_with_report_after_core_panic() -> Result<()> {
    let config = Arc::new(
        Config {
            enable_tick: false,
            enable_heartbeat: false,
            enable_elect: false,
            ..Default::default()
        }
        .validate()?,
    );
    let mut router = RaftRouter::new(config);
    router.new_raft_node(0).await;
    let node = router.get_raft_handle(&0)?;

    tracing::info!(
        node = 0,
        "panic the core, then use a request to observe its fatal error"
    );
    {
        node.external_request(|_state| panic!("injected core panic")).await?;
        assert_eq!(Fatal::Panicked, node.with_raft_state(|_state| ()).await.unwrap_err());
    }

    tracing::info!(
        node = 0,
        "shutdown must retain the panic from the earlier internal join"
    );
    {
        let report = node.shutdown_with_report().await;
        assert_eq!(TaskJoinOutcome::Panicked, report.core);
        assert_eq!(TaskJoinOutcome::Joined, report.tick);
        assert!(report.core_already_joined);
        node.shutdown().await?;
        assert_eq!(TaskJoinOutcome::Panicked, node.shutdown_with_report().await.core);
    }
    Ok(())
}
