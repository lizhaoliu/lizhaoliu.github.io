# Metaflow Deep Dive (2) - Runtimes

![img.png](../images/arrows.png)

> [Metaflow Deep Dive (1) - Static Analysis](./metaflow-deep-dive-1.md)

In my [last post](./metaflow-deep-dive-1.md), I covered Metaflow's static graph analysis. Now let's move forward with
the workflow runtime.

## Core of the Workflow

### Revisit the Entry Point

As previously mentioned, `FlowSpec` constructor starts the workflow with `cli.main(self)`, which is defined below:

```python
# metaflow/cli.py

def main(flow, args=None, handle_exceptions=True, entrypoint=None):
    # Ignore warning(s) and prevent spamming the end-user.
    # TODO: This serves as a short term workaround for RuntimeWarning(s) thrown
    # in py3.8 related to log buffering (bufsize=1).
    import warnings

    warnings.filterwarnings("ignore")
    if entrypoint is None:
        entrypoint = [sys.executable, sys.argv[0]]

    state = CliState(flow)
    state.entrypoint = entrypoint

    try:
        if args is None:
            start(auto_envvar_prefix="METAFLOW", obj=state)
        else:
            try:
                start.main(args=args, obj=state, auto_envvar_prefix="METAFLOW")
            except SystemExit as e:
                return e.code
    except MetaflowException as x:
        if handle_exceptions:
            print_metaflow_exception(x)
            sys.exit(1)
        else:
            raise
    except Exception as x:
        if handle_exceptions:
            print_unknown_exception(x)
            sys.exit(1)
        else:
            raise
```

Since `args` is not specified (`None`), the main process wraps the workflow instance into a `state` object and
calls `start(auto_envvar_prefix="METAFLOW", obj=state)`. Now move our needle to the `start` function.

### `cli.start`

This is when things get slightly more complex, as Metaflow is tightly coupled with
the [`click` library](https://palletsprojects.com/p/click/). It can be a bit tricky to get a clean view.

#### Signature

First, this function has a lot of decorators, most of which come from `click`.

```python
# metaflow/cli.py

@decorators.add_decorator_options
@click.command(
    cls=click.CommandCollection,
    sources=[cli] + plugins.get_plugin_cli(),
    invoke_without_command=True,
)
# Omitted all @click.option decorators.
@click.pass_context
def start(
        ctx,
        quiet=False,
        metadata=None,
        environment=None,
        datastore=None,
        datastore_root=None,
        decospecs=None,
        package_suffixes=None,
        pylint=None,
        coverage=None,
        event_logger=None,
        monitor=None,
        **deco_options
):
    ...
```

* `@click.command` decorator stores (the decorated) `start` as a callback into a (
  callable) `click.CommandCollection` instance, once called, it invokes `start` down the chain.
* `@click.pass_context` decorator makes a `context` object that carries global (system and user-defined) states as an
  argument to `start` (that's why you see `ctx` in the argument list, but not in actual invocation).

#### Body

Now let's look at the `start` function body.

<details>
<summary>Click to expand</summary>

```python
# metaflow/cli.py

# Decorators are omitted.
def start(
        ctx,
        quiet=False,
        metadata=None,
        environment=None,
        datastore=None,
        datastore_root=None,
        decospecs=None,
        package_suffixes=None,
        pylint=None,
        coverage=None,
        event_logger=None,
        monitor=None,
        **deco_options
):
    global echo
    if quiet:
        echo = echo_dev_null
    else:
        echo = echo_always

    ctx.obj.version = metaflow_version.get_version()
    version = ctx.obj.version
    if use_r():
        version = metaflow_r_version()

    echo("Metaflow %s" % version, fg="magenta", bold=True, nl=False)
    echo(" executing *%s*" % ctx.obj.flow.name, fg="magenta", nl=False)
    echo(" for *%s*" % resolve_identity(), fg="magenta")

    if coverage:
        from coverage import Coverage

        no_covrc = "COVERAGE_RCFILE" not in os.environ
        cov = Coverage(
            data_suffix=True,
            auto_data=True,
            source=["metaflow"] if no_covrc else None,
            branch=True if no_covrc else None,
        )
        cov.start()

    cli_args._set_top_kwargs(ctx.params)
    ctx.obj.echo = echo
    ctx.obj.echo_always = echo_always
    ctx.obj.graph = FlowGraph(ctx.obj.flow.__class__)
    ctx.obj.logger = logger
    ctx.obj.check = _check
    ctx.obj.pylint = pylint
    ctx.obj.top_cli = cli
    ctx.obj.package_suffixes = package_suffixes.split(",")
    ctx.obj.reconstruct_cli = _reconstruct_cli

    ctx.obj.event_logger = EventLogger(event_logger)

    ctx.obj.environment = [
        e for e in ENVIRONMENTS + [MetaflowEnvironment] if e.TYPE == environment
    ][0](ctx.obj.flow)
    ctx.obj.environment.validate_environment(echo)

    ctx.obj.monitor = Monitor(monitor, ctx.obj.environment, ctx.obj.flow.name)
    ctx.obj.monitor.start()

    ctx.obj.metadata = [m for m in METADATA_PROVIDERS if m.TYPE == metadata][0](
        ctx.obj.environment, ctx.obj.flow, ctx.obj.event_logger, ctx.obj.monitor
    )

    ctx.obj.datastore_impl = DATASTORES[datastore]

    if datastore_root is None:
        datastore_root = ctx.obj.datastore_impl.get_datastore_root_from_config(
            ctx.obj.echo
        )
    if datastore_root is None:
        raise CommandException(
            "Could not find the location of the datastore -- did you correctly set the "
            "METAFLOW_DATASTORE_SYSROOT_%s environment variable?" % datastore.upper()
        )

    ctx.obj.datastore_impl.datastore_root = datastore_root

    FlowDataStore.default_storage_impl = ctx.obj.datastore_impl
    ctx.obj.flow_datastore = FlowDataStore(
        ctx.obj.flow.name,
        ctx.obj.environment,
        ctx.obj.metadata,
        ctx.obj.event_logger,
        ctx.obj.monitor,
    )

    # It is important to initialize flow decorators early as some of the
    # things they provide may be used by some of the objects initialize after.
    decorators._init_flow_decorators(
        ctx.obj.flow,
        ctx.obj.graph,
        ctx.obj.environment,
        ctx.obj.flow_datastore,
        ctx.obj.metadata,
        ctx.obj.logger,
        echo,
        deco_options,
    )

    if decospecs:
        decorators._attach_decorators(ctx.obj.flow, decospecs)

    # initialize current and parameter context for deploy-time parameters
    current._set_env(flow_name=ctx.obj.flow.name, is_running=False)
    parameters.set_parameter_context(
        ctx.obj.flow.name, ctx.obj.echo, ctx.obj.flow_datastore
    )

    if ctx.invoked_subcommand not in ("run", "resume"):
        # run/resume are special cases because they can add more decorators with --with,
        # so they have to take care of themselves.
        decorators._attach_decorators(ctx.obj.flow, ctx.obj.environment.decospecs())
        decorators._init_step_decorators(
            ctx.obj.flow,
            ctx.obj.graph,
            ctx.obj.environment,
            ctx.obj.flow_datastore,
            ctx.obj.logger,
        )
        # TODO (savin): Enable lazy instantiation of package
        ctx.obj.package = None
    if ctx.invoked_subcommand is None:
        ctx.invoke(check)
```

</details>

The code is chunky, though it simply does the following:

* Puts a few runtime states into `ctx.obj`, an arbitrary object that stores user data. Notably the following are stored:
    * The workflow DAG instance.
    * A `FlowDataStore` instance, that has-a either `LocalStorage` or `S3Storage` as workflow runtime data storge
      implementation.
    * A `MetadataProvider` instance.
    * A `Monitor` instance, running as a separate _process_ (called `sidecar`) to monitor the flow and gather metrics.
* Calls `cli.run` in the end to enter the main execution lifecycle. You may not see the call of `cli.run`, this is
  because when we run `python3 branch_flow.py run`, the subcommand `run` tells `click` library to invoke that function
  internally. Notably, `ctx.obj` is also passed along.

### On `cli.run`

It is where the main show begins.

```python
# metaflow/cli.py

# decorators are omitted for brevity.
def run(
        obj,
        tags=None,
        max_workers=None,
        max_num_splits=None,
        max_log_size=None,
        decospecs=None,
        run_id_file=None,
        user_namespace=None,
        **kwargs
):
    if user_namespace is not None:
        namespace(user_namespace or None)
    before_run(obj, tags, decospecs + obj.environment.decospecs())

    runtime = NativeRuntime(
        obj.flow,
        obj.graph,
        obj.flow_datastore,
        obj.metadata,
        obj.environment,
        obj.package,
        obj.logger,
        obj.entrypoint,
        obj.event_logger,
        obj.monitor,
        max_workers=max_workers,
        max_num_splits=max_num_splits,
        max_log_size=max_log_size * 1024 * 1024,
    )
    write_latest_run_id(obj, runtime.run_id)
    write_run_id(run_id_file, runtime.run_id)

    obj.flow._set_constants(kwargs)
    runtime.persist_constants()
    runtime.execute()
```

What happens here is:

1. `before_run(...)` performs all the gate-keeping checks such as DAG validation and pylint.
2. `obj.flow._set_constants(kwargs)` sets the `Parameter` values as attributes of the workflow instance.
3. A `NativeRuntime` instance is created using all the global states carried over by `ctx.obj`.
    1. `runtime.persist_constants()` persists workflow parameters before the `start` step.
    2. `runtime.execute()` actually starts the workflow execution.

### Workflow Runtime

As mentioned earlier, `NativeRuntime` is the core component of a workflow to orchestrate the execution of the tasks.

At the heart, it runs an _event loop_ that implements _producer-consumer pattern_, where the producer is the workflow
and the consumer is the worker processes.

#### `NativeRuntime` constructor

<details>

<summary>Expand Code</summary>

```python
# metaflow/runtime.py

class NativeRuntime(object):
    def __init__(
            self,
            flow,
            graph,
            flow_datastore,
            metadata,
            environment,
            package,
            logger,
            entrypoint,
            event_logger,
            monitor,
            run_id=None,
            clone_run_id=None,
            clone_steps=None,
            max_workers=MAX_WORKERS,
            max_num_splits=MAX_NUM_SPLITS,
            max_log_size=MAX_LOG_SIZE,
    ):

        if run_id is None:
            self._run_id = metadata.new_run_id()
        else:
            self._run_id = run_id
            metadata.register_run_id(run_id)

        self._flow = flow
        self._graph = graph
        self._flow_datastore = flow_datastore
        self._metadata = metadata
        self._environment = environment
        self._logger = logger
        self._max_workers = max_workers
        self._num_active_workers = 0
        self._max_num_splits = max_num_splits
        self._max_log_size = max_log_size
        self._params_task = None
        self._entrypoint = entrypoint
        self.event_logger = event_logger
        self._monitor = monitor

        self._clone_run_id = clone_run_id
        self._clone_steps = {} if clone_steps is None else clone_steps

        self._origin_ds_set = None
        if clone_run_id:
            # resume logic
            # 0. If clone_run_id is specified, attempt to clone all the
            # successful tasks from the flow with `clone_run_id`. And run the
            # unsuccessful or not-run steps in the regular fashion.
            # 1. With _find_origin_task, for every task in the current run, we
            # find the equivalent task in `clone_run_id` using
            # pathspec_index=run/step:[index] and verify if this task can be
            # cloned.
            # 2. If yes, we fire off a clone-only task which copies the
            # metadata from the `clone_origin` to pathspec=run/step/task to
            # mimmick that the resumed run looks like an actual run.
            # 3. All steps that couldn't be cloned (either unsuccessful or not
            # run) are run as regular tasks.
            # Lastly, to improve the performance of the cloning process, we
            # leverage the TaskDataStoreSet abstraction to prefetch the
            # entire DAG of `clone_run_id` and relevant data artifacts
            # (see PREFETCH_DATA_ARTIFACTS) so that the entire runtime can
            # access the relevant data from cache (instead of going to the datastore
            # after the first prefetch).
            logger(
                "Gathering required information to resume run (this may take a bit of time)..."
            )
            self._origin_ds_set = TaskDataStoreSet(
                flow_datastore,
                clone_run_id,
                prefetch_data_artifacts=PREFETCH_DATA_ARTIFACTS,
            )
        self._run_queue = []
        self._poll = procpoll.make_poll()
        self._workers = {}  # fd -> subprocess mapping
        self._finished = {}
        self._is_cloned = {}
        # NOTE: In case of unbounded foreach, we need the following to schedule
        # the (sibling) mapper tasks  of the control task (in case of resume);
        # and ensure that the join tasks runs only if all dependent tasks have
        # finished.
        self._control_num_splits = {}  # control_task -> num_splits mapping

        for step in flow:
            for deco in step.decorators:
                deco.runtime_init(flow, graph, package, self._run_id)
```

</details>

1. A unique `run_id` is generated for the new run. As for a local run, it simply uses the current timestamp.
2. A `ProcPoll` instance is created. It is used to track (by polling at a fixed interval) the worker processes.

#### Dissecting `NativeRuntime.execute`

<details>
<summary>Expand Code</summary>

```python
# metaflow/runtime.py

class NativeRuntime(Runtime):
    def execute(self):
        self._logger("Workflow starting (run-id %s):" % self._run_id, system_msg=True)

        self._metadata.start_run_heartbeat(self._flow.name, self._run_id)

        if self._params_task:
            self._queue_push("start", {"input_paths": [self._params_task]})
        else:
            self._queue_push("start", {})

        progress_tstamp = time.time()
        try:
            # main scheduling loop
            exception = None
            while self._run_queue or self._num_active_workers > 0:

                # 1. are any of the current workers finished?
                finished_tasks = list(self._poll_workers())
                # 2. push new tasks triggered by the finished tasks to the queue
                self._queue_tasks(finished_tasks)
                # 3. if there are available worker slots, pop and start tasks
                #    from the queue.
                self._launch_workers()

                if time.time() - progress_tstamp > PROGRESS_INTERVAL:
                    progress_tstamp = time.time()
                    msg = "%d tasks are running: %s." % (
                        self._num_active_workers,
                        "e.g. ...",
                    )  # TODO
                    self._logger(msg, system_msg=True)
                    msg = "%d tasks are waiting in the queue." % len(self._run_queue)
                    self._logger(msg, system_msg=True)
                    msg = "%d steps are pending: %s." % (0, "e.g. ...")  # TODO
                    self._logger(msg, system_msg=True)

        except KeyboardInterrupt as ex:
            self._logger("Workflow interrupted.", system_msg=True, bad=True)
            self._killall()
            exception = ex
            raise
        except Exception as ex:
            self._logger("Workflow failed.", system_msg=True, bad=True)
            self._killall()
            exception = ex
            raise
        finally:
            # on finish clean tasks
            for step in self._flow:
                for deco in step.decorators:
                    deco.runtime_finished(exception)

            self._metadata.stop_heartbeat()

        # assert that end was executed and it was successful
        if ("end", ()) in self._finished:
            self._logger("Done!", system_msg=True)
        else:
            raise MetaflowInternalError(
                "The *end* step was not successful " "by the end of flow."
            )
```

</details>

Key points to note:

1. This method (event loop) is not thread-safe and should be run by a single thread.
2. `self._metadata.start_run_heartbeat(..)` starts a heartbeat service for the run. But it is a no-op if only running
   locally.
3. `self._queue_push("start", ..)` enqueues the seeding `start` task to the `queue`.
4. The `while` loop is the main event loop. As long as there are outstanding `steps` in the `queue`, or there are active
   workers in the pool, the loop continues:
    1. `finished_tasks = list(self._poll_workers())` fetches the previously finished tasks from the worker pool.
    2. `self._queue_tasks(finished_tasks)` pushes the child `steps` of the finished tasks to the `queue`.
    3. `self._launch_workers()` for each `step` remaining in the `queue`, dequeues it and creates a new worker
       _process_ (if the worker pool capacity is not reached) to run.
    5. Should there be an unexpected exception, the loop kills all workers and exits.
