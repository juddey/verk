defmodule Verk.WorkersManager do
  @moduledoc """
  A WorkersManager assign jobs to workers from a pool (handled by poolboy) monitoring the job.

  It interacts with the related QueueManager to request jobs and to schedule jobs to be retried
  """

  use GenServer
  require Logger
  alias Verk.{Events, Job, QueueManager, Log, Time, QueueConsumer}

  @default_timeout 1000

  defmodule State do
    @moduledoc false
    defstruct queue_name: nil,
              pool_name: nil,
              queue_manager_name: nil,
              consumer_name: nil,
              pool_size: nil,
              monitors: nil,
              timeout: nil,
              status: :running
  end

  @doc """
  Returns the atom that represents the WorkersManager of the `queue`
  """
  @spec name(binary | atom) :: atom
  def name(queue) do
    String.to_atom("#{queue}.workers_manager")
  end

  @doc false
  def start_link(workers_manager_name, queue_name, queue_manager_name, pool_name, pool_size) do
    gen_server_args = [workers_manager_name, queue_name, queue_manager_name, pool_name, pool_size]
    GenServer.start_link(__MODULE__, gen_server_args, name: workers_manager_name)
  end

  @doc """
  List running jobs

  Example:
      [%{process: #PID<0.186.0>, job: %Verk.Job{...}, started_at: %DateTime{...}} ]
  """
  @spec running_jobs(binary | atom) :: Map.t()
  def running_jobs(queue, limit \\ 100) do
    match_spec = [{{:"$1", :_, :"$2", :_, :"$3"}, [], [{{:"$1", :"$2", :"$3"}}]}]

    case :ets.select(name(queue), match_spec, limit) do
      :"$end_of_table" ->
        []

      {jobs, _continuation} ->
        for {pid, job, started_at} <- jobs do
          %{process: pid, job: job, started_at: started_at}
        end
    end
  end

  @process_info_keys [:current_stacktrace, :initial_call, :reductions, :status]
  @doc """
  List information about the process that is currently running a `job_id`
  """
  @spec inspect_worker(binary | atom, binary) :: {:ok, Map.t()} | {:error, :not_found}
  def inspect_worker(queue, job_id) do
    case :ets.match(name(queue), {:"$1", job_id, :"$2", :_, :"$3"}) do
      [] ->
        {:error, :not_found}

      [[pid, job, started_at]] ->
        info = Process.info(pid, @process_info_keys)

        if info do
          {:ok, %{process: pid, job: job, started_at: started_at, info: info}}
        else
          {:error, :not_found}
        end
    end
  end

  @doc """
  Pauses a `queue`
  """
  @spec pause(binary | atom) :: :ok | :already_paused
  def pause(queue), do: GenServer.call(name(queue), :pause)

  @doc """
  Resumes a `queue`.
  """
  @spec resume(binary | atom) :: :ok | :already_running
  def resume(queue), do: GenServer.call(name(queue), :resume)

  @doc """
  Create a table to monitor workers saving data about the assigned queue/pool
  """
  def init([workers_manager_name, queue_name, queue_manager_name, pool_name, size]) do
    monitors = :ets.new(workers_manager_name, [:named_table, read_concurrency: true])
    timeout = Confex.get_env(:verk, :workers_manager_timeout, @default_timeout)
    status = Verk.Manager.status(queue_name)

    consumer_name = QueueConsumer.name(queue_name)
    {:ok, _pid} = QueueConsumer.start_link(consumer_name, queue_name)

    state = %State{
      queue_name: queue_name,
      queue_manager_name: queue_manager_name,
      consumer_name: consumer_name,
      pool_name: pool_name,
      pool_size: size,
      monitors: monitors,
      timeout: timeout,
      status: status
    }

    Logger.info("Workers Manager started for queue #{queue_name} (#{status})")

    if status == :running, do: notify!(%Events.QueueRunning{queue: queue_name})

    QueueConsumer.ask(consumer_name, size)

    {:ok, state}
  end

  @doc false
  def handle_call(:pause, _from, state = %State{status: :running}) do
    notify!(%Events.QueuePausing{queue: state.queue_name})
    {:reply, :ok, %{state | status: :pausing}}
  end

  def handle_call(:pause, _from, state = %State{status: :paused}),
    do: {:reply, :already_paused, state}

  def handle_call(:pause, _from, state), do: {:reply, :ok, state}

  def handle_call(:resume, _from, state = %State{status: :running}),
    do: {:reply, :already_running, state}

  def handle_call(:resume, _from, state) do
    notify!(%Events.QueueRunning{queue: state.queue_name})
    {:reply, :ok, %{state | status: :running}}
  end

  def handle_info({:jobs, jobs}, state) do
    Enum.each(jobs, fn [item_id, ["job", job]] ->
      case job |> Job.decode(item_id) do
        {:ok, verk_job} ->
          start_job(verk_job, state)

        {:error, error} ->
          Logger.error(
            "Failed to decode job, error: #{inspect(error)}, original job: #{inspect(job)}"
          )

          QueueManager.malformed(state.queue_manager_name, job)
      end
    end)

    {:noreply, state}
  end

  # Possible reasons to receive a :DOWN message:
  #   * :normal - The worker finished the job but :done message did not arrive
  #   * :failed - The worker failed the job but :failed message did not arrive
  #   * {reason, stack } - The worker crashed and it has a stacktrace
  #   * reason - The worker crashed and it doesn't have have a stacktrace
  def handle_info({:DOWN, mref, _, worker, :normal}, state) do
    case :ets.lookup(state.monitors, worker) do
      [{^worker, _job_id, job, ^mref, start_time}] ->
        succeed(job, start_time, worker, mref, state.monitors, state.queue_manager_name, state.consumer_name)

      _ ->
        Logger.warn("Worker finished but such worker was not monitored #{inspect(worker)}")
    end

    {:noreply, state}
  end

  def handle_info({:DOWN, mref, _, worker, :failed}, state) do
    handle_down!(mref, worker, :failed, [], state)
    {:noreply, state}
  end

  def handle_info({:DOWN, mref, _, worker, {reason, stack}}, state) do
    handle_down!(mref, worker, reason, stack, state)
    {:noreply, state}
  end

  def handle_info({:DOWN, mref, _, worker, reason}, state) do
    handle_down!(mref, worker, reason, [], state)
    {:noreply, state}
  end

  defp handle_down!(mref, worker, reason, stack, state) do
    Logger.debug("Worker got down, reason: #{inspect(reason)}, #{inspect([mref, worker])}")
    :ok = :poolboy.checkin(state.pool_name, worker)

    case :ets.lookup(state.monitors, worker) do
      [{^worker, _job_id, job, ^mref, start_time}] ->
        exception = RuntimeError.exception(inspect(reason))

        fail(
          job,
          start_time,
          worker,
          mref,
          state.monitors,
          state.queue_manager_name,
          state.consumer_name,
          exception,
          stack
        )

      error ->
        Logger.warn("Worker got down but it was not found, error: #{inspect(error)}")
    end
  end

  @doc false
  def handle_cast({:done, worker, job_id}, state) do
    :ok = :poolboy.checkin(state.pool_name, worker)

    case :ets.lookup(state.monitors, worker) do
      [{^worker, ^job_id, job, mref, start_time}] ->
        succeed(job, start_time, worker, mref, state.monitors, state.queue_manager_name, state.consumer_name)

      _ ->
        Logger.warn("#{job_id} finished but no worker was monitored")
    end

    {:noreply, state}
  end

  def handle_cast({:failed, worker, job_id, exception, stacktrace}, state) do
    Logger.debug("Job failed reason: #{inspect(exception)}")
    :ok = :poolboy.checkin(state.pool_name, worker)

    case :ets.lookup(state.monitors, worker) do
      [{^worker, ^job_id, job, mref, start_time}] ->
        fail(
          job,
          start_time,
          worker,
          mref,
          state.monitors,
          state.queue_manager_name,
          state.consumer_name,
          exception,
          stacktrace
        )

      _ ->
        Logger.warn("#{job_id} failed but no worker was monitored")
    end

    {:noreply, state}
  end

  defp succeed(job, start_time, worker, mref, monitors, queue_manager_name, consumer_name) do
    QueueManager.ack(queue_manager_name, job.item_id)
    QueueConsumer.ask(consumer_name, 1)
    Log.done(job, start_time, worker)
    demonitor!(monitors, worker, mref)
    finished_at = Time.now()
    job = %{job | finished_at: finished_at}
    notify!(%Events.JobFinished{job: job, started_at: start_time, finished_at: finished_at})
  end

  defp fail(job, start_time, worker, mref, monitors, queue_manager_name, consumer_name, exception, stacktrace) do
    Log.fail(job, start_time, worker)
    demonitor!(monitors, worker, mref)
    :ok = QueueManager.retry(queue_manager_name, job, exception, stacktrace)
    :ok = QueueManager.ack(queue_manager_name, job.item_id)
    QueueConsumer.ask(consumer_name, 1)

    notify!(%Events.JobFailed{
      job: job,
      started_at: start_time,
      failed_at: Time.now(),
      exception: exception,
      stacktrace: stacktrace
    })
  end

  defp start_job(job, state) do
    case :poolboy.checkout(state.pool_name, false) do
      worker when is_pid(worker) ->
        monitor!(state.monitors, worker, job)
        Log.start(job, worker)
        Verk.Worker.perform_async(worker, self(), job)
        notify!(%Events.JobStarted{job: job, started_at: Time.now()})
    end
  end

  defp demonitor!(monitors, worker, mref) do
    true = Process.demonitor(mref, [:flush])
    true = :ets.delete(monitors, worker)
  end

  defp monitor!(monitors, worker, job = %Job{jid: job_id}) do
    mref = Process.monitor(worker)
    now = Time.now()
    true = :ets.insert(monitors, {worker, job_id, job, mref, now})
  end

  @doc false
  def terminate(reason, _state) do
    Logger.error("Manager terminating, reason: #{inspect(reason)}")
    :ok
  end

  defp notify!(event) do
    :ok = Verk.EventProducer.async_notify(event)
  end
end
