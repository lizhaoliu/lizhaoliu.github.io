# Metaflow Deep Dive (2) - Runtime

![img.png](../images/arrows.png)

Previous Post(s):
> [Metaflow Deep Dive (1) - Static Analysis](./metaflow-deep-dive-1.md)

In my [last post](./metaflow-deep-dive-1.md), I covered Metaflow's static graph analysis. Now let's move forward with
the workflow runtime.

## Core of the Workflow Runtime

Metaflow implemented its own process worker pool based on *NIX system polling mechanism.
This [documentation](https://github.com/lizhaoliu/metaflow/blob/master/docs/concurrency.md) explains why Metaflow made
such choice.

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

1. The main process packs the workflow instance and entry point (interpreter binary and script paths) into a `state`
   object.
2. Calls `start(auto_envvar_prefix="METAFLOW", obj=state)` to kick off the workflow.

Now let's move to the `start` function.

### `start`

This is when things get slightly more complex, as Metaflow tightly couples with
the [`click` library](https://palletsprojects.com/p/click/). It can be a bit tricky to get a clean view.

#### Decorators

First, it has a lot of decorators, most of which come from `click`.

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
        ...
):
    ...
```

* `@click.command` decorator stores (the decorated) `start` function as a callback into a `click.CommandCollection`
  instance (a functor) which, once called, invokes `start` down the chain.
* `sources=[cli] + plugins.get_plugin_cli()` combines subcommands defined by `cli` group, which includes the `run` command that we use to start the execution.
* `@click.pass_context` decorator creates a `context` object that carries global (system and user-defined) states as an
  argument to `start` (that's why you see `ctx` in the argument list, but not in invocation). It also adds an `obj`
  argument, and that's how the `state` instance is passed in.

#### Definition

Now moving on to the `start` function body.

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

Despite being chunky, the code simply does the following:

* Packs a few runtime states into `ctx.obj`, an arbitrary object that stores user data. Notably the following are
  getting stored:
    * The workflow DAG instance.
    * A `FlowDataStore` instance, that has-a either `LocalStorage` or `S3Storage` as workflow runtime data storge
      implementation.
    * A `MetadataProvider` instance, which I will cover later.
    * A `Monitor` instance, running as a separate _process_ (called `sidecar`) to monitor the flow and gather metrics.
* Calls `cli.run` in the end to enter the main execution lifecycle. You may not see the call of `cli.run`, this is
  because when we run `python3 branch_flow.py run`, the subcommand `run` tells `click` library to invoke that function
  internally. Notably, `ctx.obj` is also passed along.

### On `run`

This is where the main show begins.

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
    1. `runtime.persist_constants()` persists workflow parameters before the `start` step fires off.
    2. `runtime.execute()` starts the workflow execution.

### Workflow Runtime

As mentioned earlier, `NativeRuntime` is the core component of a workflow to orchestrate the execution of the tasks.

At its heart, a runtime runs an _event loop_ that implements _producer-consumer pattern_, where the producer is the
workflow topology and the consumer is the worker processes. Runtime is the pump that drives the workflow execution.

#### `NativeRuntime` constructor

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

1. A unique `run_id` is generated for the new run. As for a local run, it simply uses the current timestamp.
2. A `ProcPoll` instance is created. It is used to track (by polling at a fixed interval) the worker processes.

#### Dissecting `NativeRuntime.execute`

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
                # 3. if there are available worker slots, pop and start tasks from the queue.
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
            raise MetaflowInternalError("The *end* step was not successful " "by the end of flow.")
```

Summary:

1. The event loop is not thread-safe and should be run by a single thread.
2. `self._metadata.start_run_heartbeat(..)` starts a heartbeat service for the run. But it is a no-op if only running
   locally.
3. `self._queue_push("start", ..)` enqueues the seeding `start` task to the `queue`.
4. The `while` loop is the main event loop. As long as there are outstanding `steps` in the `queue`, or there are active
   workers in the pool, the loop continues:
    1. `finished_tasks = list(self._poll_workers())` fetches the previously finished tasks from the worker pool.
    2. `self._queue_tasks(finished_tasks)` pushes the child `steps` of the finished tasks to the `queue`.
    3. `self._launch_workers()`: for each `step` remaining in the `queue`, dequeues it and creates a new worker
       _process_ (if the worker pool capacity is not reached) to run.
    5. Should there be an unexpected exception, the loop terminates all in-flight workers and exits.

Alright, that's it! Now let's move on to the rest of the code.

#### Notes on `_poll_workers()`

```python
# metaflow/runtime.py

class NativeRuntime(Runtime):
    def _poll_workers(self):
        if self._workers:
            for event in self._poll.poll(PROGRESS_INTERVAL):
                worker = self._workers.get(event.fd)
                if worker:
                    if event.can_read:
                        worker.read_logline(event.fd)
                    if event.is_terminated:
                        returncode = worker.terminate()

                        for fd in worker.fds():
                            self._poll.remove(fd)
                            del self._workers[fd]
                        self._num_active_workers -= 1

                        task = worker.task
                        if returncode:
                            # worker did not finish successfully
                            if worker.cleaned or returncode == METAFLOW_EXIT_DISALLOW_RETRY:
                                self._logger(
                                    "This failed task will not be " "retried.",
                                    system_msg=True,
                                )
                            else:
                                if task.retries < task.user_code_retries + task.error_retries:
                                    self._retry_worker(worker)
                                else:
                                    raise TaskFailed(task)
                        else:
                            # worker finished successfully
                            yield task
```

1. Keeps polling the worker pool at a fixed rate for any I/O events.
2. Once an event emerges, it retrieves the associated worker (by `fd`).
3. If that event is in terminated state (task finished or failed), the worker is removed from the pool.
4. Depending on the return code, the task is either returned (finished) or retried (failed).

### Task

`Task` represents a single `step` in the workflow. It packs all data that is needed to be plugged into a worker.

### Worker

`Worker` is the execution unit of the workflow. It is responsible for executing a single task (`step`) in a dedicated
_process_.

#### Constructor

```python
# metaflow/runtime.py

class Worker(object):
    def __init__(self, task: Task, max_logs_size: int):

        self.task = task
        self._proc = self._launch()

        if task.retries > task.user_code_retries:
            self.task.log(
                "Task fallback is starting to handle the failure.",
                system_msg=True,
                pid=self._proc.pid,
            )
        elif not task.is_cloned:
            suffix = " (retry)." if task.retries else "."
            self.task.log(
                "Task is starting" + suffix, system_msg=True, pid=self._proc.pid
            )

        self._stdout = TruncatedBuffer("stdout", max_logs_size)
        self._stderr = TruncatedBuffer("stderr", max_logs_size)

        self._logs = {
            self._proc.stderr.fileno(): (self._proc.stderr, self._stderr),
            self._proc.stdout.fileno(): (self._proc.stdout, self._stdout),
        }

        self._encoding = sys.stdout.encoding or "UTF-8"
        self.killed = False  # Killed indicates that the task was forcibly killed
        # with SIGKILL by the master process.
        # A killed task is always considered cleaned
        self.cleaned = False  # A cleaned task is one that is shutting down and has been
        # noticed by the runtime and queried for its state (whether or
        # not is is properly shut down)
```

Once a worker is instantiated, an underlying process is spawn to run the task.

#### Process Creation

```python
# metaflow/runtime.py

class Worker(object):
    def _launch(self):
        args = CLIArgs(self.task)
        env = dict(os.environ)

        if self.task.clone_run_id:
            args.command_options["clone-run-id"] = self.task.clone_run_id

        if self.task.is_cloned and self.task.clone_origin:
            args.command_options["clone-only"] = self.task.clone_origin
            # disabling atlas sidecar for cloned tasks due to perf reasons
            args.top_level_options["monitor"] = "nullSidecarMonitor"
        else:
            # decorators may modify the CLIArgs object in-place
            for deco in self.task.decos:
                deco.runtime_step_cli(
                    args,
                    self.task.retries,
                    self.task.user_code_retries,
                    self.task.ubf_context,
                )
        env.update(args.get_env())
        env["PYTHONUNBUFFERED"] = "x"
        # NOTE bufsize=1 below enables line buffering which is required
        # by read_logline() below that relies on readline() not blocking
        # print('running', args)
        cmdline = args.get_args()
        debug.subcommand_exec(cmdline)
        return subprocess.Popen(
            cmdline,
            env=env,
            bufsize=1,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
```

This unveils how a worker process is spawned:

1. `args = CLIArgs(self.task)` constructs the command line arguments from the task specification, they are used to spawn
   a worker process. Notably:
    1. Entry point of the arguments are [`python_interpreter_path`, `main_script_path`].
    2. It adds "step" to the CLI args as a subcommand, so that `cli.step` function will be called (
       by `click` framework) in the worker process to run that specific `step`.
2. `subprocess.Popen(..)` spawns a process object with the CLI arguments and env vars.

### cli.step

This is where a `step` code is retrieved and executed.

```python
# metaflow/cli.py

@cli.command(help="Internal command to execute a single task.")
@click.argument("step-name")
# Omitted @click.option decorators.
@click.pass_context
def step(
        ctx,
        step_name,
        # Omitted other kwargs.
):
    if ubf_context == "none":
        ubf_context = None
    if opt_namespace is not None:
        namespace(opt_namespace or None)

    func = None
    try:
        func = getattr(ctx.obj.flow, step_name)
    except:
        raise CommandException("Step *%s* doesn't exist." % step_name)
    if not func.is_step:
        raise CommandException("Function *%s* is not a step." % step_name)
    echo("Executing a step, *%s*" % step_name, fg="magenta", bold=False)

    if decospecs:
        decorators._attach_decorators_to_step(func, decospecs)

    step_kwargs = ctx.params
    # Remove argument `step_name` from `step_kwargs`.
    step_kwargs.pop("step_name", None)
    # Remove `opt_*` prefix from (some) option keys.
    step_kwargs = dict(
        [(k[4:], v) if k.startswith("opt_") else (k, v) for k, v in step_kwargs.items()]
    )
    cli_args._set_step_kwargs(step_kwargs)

    ctx.obj.metadata.add_sticky_tags(tags=opt_tag)
    paths = decompress_list(input_paths) if input_paths else []

    task = MetaflowTask(
        ctx.obj.flow,
        ctx.obj.flow_datastore,
        ctx.obj.metadata,
        ctx.obj.environment,
        ctx.obj.echo,
        ctx.obj.event_logger,
        ctx.obj.monitor,
        ubf_context,
    )
    if clone_only:
        task.clone_only(step_name, run_id, task_id, clone_only, retry_count)
    else:
        task.run_step(
            step_name,
            run_id,
            task_id,
            clone_run_id,
            paths,
            split_index,
            retry_count,
            max_user_code_retries,
        )

    echo("Success", fg="green", bold=True, indent=True)
```

Key takeaways:

1. The decorator `@click.argument("step-name")` adds a `step_name` argument to the CLI.
2. `func = getattr(ctx.obj.flow, step_name)` fetches the `step` function from the flow object. And
   then `decorators._attach_decorators_to_step(func, decospecs)` attaches additional decorators to that `step`
   function.
3. A `MetaflowTask` object is created to execute the `step`.

### MetaflowTask

> A `MetaflowTask` prepares a Flow instance for execution of a single step.

#### `MetaflowTask.run_step`

This is the place where a `step` function is eventually executed. The method is chunky, so I'll divide it into smaller
pieces.

```python
# metaflow/task.py
def run_step(
        self,
        step_name,
        run_id,
        task_id,
        origin_run_id,
        input_paths,
        split_index,
        retry_count,
        max_user_code_retries,
):
    if run_id and task_id:
        self.metadata.register_run_id(run_id)
        self.metadata.register_task_id(run_id, step_name, task_id, retry_count)
    else:
        raise MetaflowInternalError(
            "task.run_step needs a valid run_id " "and task_id"
        )

    if retry_count >= MAX_ATTEMPTS:
        # any results with an attempt ID >= MAX_ATTEMPTS will be ignored
        # by datastore, so running a task with such a retry_could would
        # be pointless and dangerous
        raise MetaflowInternalError(
            "Too many task attempts (%d)! " "MAX_ATTEMPTS exceeded." % retry_count
        )

    metadata_tags = ["attempt_id:{0}".format(retry_count)]
    self.metadata.register_metadata(
        run_id,
        step_name,
        task_id,
        [
            MetaDatum(
                field="attempt",
                value=str(retry_count),
                type="attempt",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="origin-run-id",
                value=str(origin_run_id),
                type="origin-run-id",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="ds-type",
                value=self.flow_datastore.TYPE,
                type="ds-type",
                tags=metadata_tags,
            ),
            MetaDatum(
                field="ds-root",
                value=self.flow_datastore.datastore_root,
                type="ds-root",
                tags=metadata_tags,
            ),
        ],
    )

    step_func = getattr(self.flow, step_name)
    decorators = step_func.decorators

    node = self.flow._graph[step_name]
    join_type = None
    if node.type == "join":
        join_type = self.flow._graph[node.split_parents[-1]].type
```

It denies excessive retires, collects and registers metadata, and fetches all decorators of the `step` function.

```python
# metaflow/task.py

    # 1. initialize output datastore
    out_ds = self.flow_datastore.get_task_datastore(
        run_id, step_name, task_id, attempt=retry_count, mode="w"
    )

    out_ds.init_task()

    if input_paths:
        # 2. initialize input datastores
        inputs = self._init_data(run_id, join_type, input_paths)

        # 3. initialize foreach state
        self._init_foreach(step_name, join_type, inputs, split_index)

    # 4. initialize the current singleton
    current._set_env(
        flow=self.flow,
        run_id=run_id,
        step_name=step_name,
        task_id=task_id,
        retry_count=retry_count,
        origin_run_id=origin_run_id,
        namespace=resolve_identity(),
        username=get_username(),
        is_running=True,
    )
```

An output datastore is initialized, and the input datastore is initialized. The foreach state is initialized.

```python
# metaflow/task.py

    # 5. run task
    out_ds.save_metadata(
        {
            "task_begin": {
                "code_package_sha": os.environ.get("METAFLOW_CODE_SHA"),
                "code_package_ds": os.environ.get("METAFLOW_CODE_DS"),
                "code_package_url": os.environ.get("METAFLOW_CODE_URL"),
                "retry_count": retry_count,
            }
        }
    )
    logger = self.event_logger
    start = time.time()
    self.metadata.start_task_heartbeat(self.flow.name, run_id, step_name, task_id)
    try:
        # init side cars
        logger.start()

        msg = {
            "task_id": task_id,
            "msg": "task starting",
            "step_name": step_name,
            "run_id": run_id,
            "flow_name": self.flow.name,
            "ts": round(time.time()),
        }
        logger.log(msg)

        self.flow._current_step = step_name
        self.flow._success = False
        self.flow._task_ok = None
        self.flow._exception = None
        # Note: All internal flow attributes (ie: non-user artifacts)
        # should either be set prior to running the user code or listed in
        # FlowSpec._EPHEMERAL to allow for proper merging/importing of
        # user artifacts in the user's step code.

        if join_type:
            # Join step:

            # Ensure that we have the right number of inputs. The
            # foreach case is checked above.
            if join_type != "foreach" and len(inputs) != len(node.in_funcs):
                raise MetaflowDataMissing(
                    "Join *%s* expected %d "
                    "inputs but only %d inputs "
                    "were found" % (step_name, len(node.in_funcs), len(inputs))
                )

            # Multiple input contexts are passed in as an argument
            # to the step function.
            input_obj = Inputs(self._clone_flow(inp) for inp in inputs)
            self.flow._set_datastore(output)
            # initialize parameters (if they exist)
            # We take Parameter values from the first input,
            # which is always safe since parameters are read-only
            current._update_env(
                {"parameter_names": self._init_parameters(inputs[0], passdown=True)}
            )
        else:
            # Linear step:
            # We are running with a single input context.
            # The context is embedded in the flow.
            if len(inputs) > 1:
                # This should be captured by static checking but
                # let's assert this again
                raise MetaflowInternalError(
                    "Step *%s* is not a join "
                    "step but it gets multiple "
                    "inputs." % step_name
                )
            self.flow._set_datastore(inputs[0])
            if input_paths:
                # initialize parameters (if they exist)
                # We take Parameter values from the first input,
                # which is always safe since parameters are read-only
                current._update_env(
                    {
                        "parameter_names": self._init_parameters(
                            inputs[0], passdown=False
                        )
                    }
                )

        for deco in decorators:

            deco.task_pre_step(
                step_name,
                output,
                self.metadata,
                run_id,
                task_id,
                self.flow,
                self.flow._graph,
                retry_count,
                max_user_code_retries,
                self.ubf_context,
                inputs,
            )

            # decorators can actually decorate the step function,
            # or they can replace it altogether. This functionality
            # is used e.g. by catch_decorator which switches to a
            # fallback code if the user code has failed too many
            # times.
            step_func = deco.task_decorate(
                step_func,
                self.flow,
                self.flow._graph,
                retry_count,
                max_user_code_retries,
                self.ubf_context,
            )

        if join_type:
            self._exec_step_function(step_func, input_obj)
        else:
            self._exec_step_function(step_func)

        for deco in decorators:
            deco.task_post_step(
                step_name,
                self.flow,
                self.flow._graph,
                retry_count,
                max_user_code_retries,
            )

        self.flow._task_ok = True
        self.flow._success = True

    except Exception as ex:
        tsk_msg = {
            "task_id": task_id,
            "exception_msg": str(ex),
            "msg": "task failed with exception",
            "step_name": step_name,
            "run_id": run_id,
            "flow_name": self.flow.name,
        }
        logger.log(tsk_msg)

        exception_handled = False
        for deco in decorators:
            res = deco.task_exception(
                ex,
                step_name,
                self.flow,
                self.flow._graph,
                retry_count,
                max_user_code_retries,
            )
            exception_handled = bool(res) or exception_handled

        if exception_handled:
            self.flow._task_ok = True
        else:
            self.flow._task_ok = False
            self.flow._exception = MetaflowExceptionWrapper(ex)
            print("%s failed:" % self.flow, file=sys.stderr)
            raise

    finally:
        if self.ubf_context == UBF_CONTROL:
            self._finalize_control_task()

        end = time.time() - start

        msg = {
            "task_id": task_id,
            "msg": "task ending",
            "step_name": step_name,
            "run_id": run_id,
            "flow_name": self.flow.name,
            "ts": round(time.time()),
            "runtime": round(end),
        }
        logger.log(msg)

        attempt_ok = str(bool(self.flow._task_ok))
        self.metadata.register_metadata(
            run_id,
            step_name,
            task_id,
            [
                MetaDatum(
                    field="attempt_ok",
                    value=attempt_ok,
                    type="internal_attempt_status",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            ],
        )

        out_ds.save_metadata({"task_end": {}})
        out_ds.persist(self.flow)

        # this writes a success marker indicating that the
        # "transaction" is done
        out_ds.done()

        # final decorator hook: The task results are now
        # queryable through the client API / datastore
        for deco in decorators:
            deco.task_finished(
                step_name,
                self.flow,
                self.flow._graph,
                self.flow._task_ok,
                retry_count,
                max_user_code_retries,
            )

        # terminate side cars
        logger.terminate()
        self.metadata.stop_heartbeat()
```

1. In `try` block:
   1. Each decorator does pre-proc, and decorates the `step` function.
   2. The `step` is executed within the process.
   3. Each decorator performs post-proc.
2. In case of an exception, each decorator procs the exception, and if the exception is not considered handled, the task
   fails and the exception is re-raised. These states are also registered to the flow object.
3. In `finally` block, metadata and current flow state are persisted, and each decorator does on-finish task.

#### Persisting the Flow State

A sneak peek at the code that persists the flow state. We see that all the class and instance variables are persisted
except for:
* Properties.
* Those of `Parameter` type.

```python
# metaflow/datastore/task_datastore.py

class TaskDataStore(object):
    @only_if_not_done
    @require_mode("w")
    def persist(self, flow):
        """
        Persist any new artifacts that were produced when running flow

        NOTE: This is a DESTRUCTIVE operation that deletes artifacts from
        the given flow to conserve memory. Don't rely on artifact attributes
        of the flow object after calling this function.

        Parameters
        ----------
        flow : FlowSpec
            Flow to persist
        """

        if flow._datastore:
            self._objects.update(flow._datastore._objects)
            self._info.update(flow._datastore._info)

        # we create a list of valid_artifacts in advance, outside of
        # artifacts_iter so we can provide a len_hint below
        valid_artifacts = []
        for var in dir(flow):
            if var.startswith("__") or var in flow._EPHEMERAL:
                continue
            # Skip over properties of the class (Parameters or class variables)
            if hasattr(flow.__class__, var) and isinstance(
                    getattr(flow.__class__, var), property
            ):
                continue

            val = getattr(flow, var)
            if not (
                    isinstance(val, MethodType)
                    or isinstance(val, FunctionType)
                    or isinstance(val, Parameter)
            ):
                valid_artifacts.append((var, val))

        def artifacts_iter():
            # we consume the valid_artifacts list destructively to
            # make sure we don't keep references to artifacts. We
            # want to avoid keeping original artifacts and encoded
            # artifacts in memory simultaneously
            while valid_artifacts:
                var, val = valid_artifacts.pop()
                if not var.startswith("_") and var != "name":
                    # NOTE: Destructive mutation of the flow object. We keep
                    # around artifacts called 'name' and anything starting with
                    # '_' as they are used by the Metaflow runtime.
                    delattr(flow, var)
                yield var, val

        self.save_artifacts(artifacts_iter(), len_hint=len(valid_artifacts))
```
