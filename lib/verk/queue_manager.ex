defmodule Verk.QueueManager do
  @moduledoc """
  QueueManager handles consumption, acknowledgment and retry of a queue
  """

  use GenServer
  require Logger
  alias Verk.{Queue, DeadSet, RetrySet, Time, Job, Redis, Node}

  @default_stacktrace_size 5
  @max_jobs 25

  @doc """
  Returns the atom that represents the QueueManager of the `queue`
  """
  @spec name(binary | atom) :: atom
  def name(queue) do
    String.to_atom("#{queue}.queue_manager")
  end

  @doc false
  def start_link(queue_manager_name, queue_name) do
    GenServer.start_link(__MODULE__, [queue_name], name: queue_manager_name)
  end

  @doc """
  Add job to be retried in the assigned queue
  """
  def retry(queue_manager, job, exception, stacktrace, timeout \\ 5000) do
    now = Time.now() |> DateTime.to_unix()
    GenServer.call(queue_manager, {:retry, job, now, exception, stacktrace}, timeout)
  catch
    :exit, {:timeout, _} -> :timeout
  end

  @doc """
  Acknowledge that a job was processed
  """
  def ack(queue_manager, item_id) do
    GenServer.cast(queue_manager, {:ack, item_id})
  end

  @doc """
  Remove a malformed job from the inprogress queue
  """
  def malformed(queue_manager, item_id) do
    GenServer.cast(queue_manager, {:malformed, item_id})
  end

  @doc false
  def init([queue_name]) do
    Verk.Scripts.load(Redis.random())
    send(self(), :reenqueue_dead_jobs)
    {:ok, queue_name}
  end

  def handle_call({:retry, job, failed_at, exception, stacktrace}, _from, queue_name) do
    retry_count = (job.retry_count || 0) + 1
    job = build_retry_job(job, retry_count, failed_at, exception, stacktrace)

    if retry_count <= (job.max_retry_count || Job.default_max_retry_count()) do
      RetrySet.add!(job, failed_at, Redis.random())
    else
      Logger.info("Max retries reached to job_id #{job.jid}, job: #{inspect(job)}")
      DeadSet.add!(job, failed_at, Redis.random())
    end

    {:reply, :ok, queue_name}
  end

  defp build_retry_job(job, retry_count, failed_at, exception, stacktrace) do
    job = %{
      job
      | error_backtrace: format_stacktrace(stacktrace),
        error_message: Exception.message(exception),
        retry_count: retry_count
    }

    if retry_count > 1 do
      # Set the retried_at if this job was already retried at least once
      %{job | retried_at: failed_at}
    else
      # Set the failed_at if this the first time the job failed
      %{job | failed_at: failed_at}
    end
  end

  @doc false
  def handle_cast({:ack, item_id}, queue_name) do
    case Queue.delete_job(queue_name, item_id, Redis.random()) do
      {:ok, true} -> :ok
      _ -> Logger.error("Failed to acknowledge job #{inspect(item_id)}")
    end

    {:noreply, queue_name}
  end

  @doc false
  def handle_cast({:malformed, item_id}, queue_name) do
    case Queue.delete_job(queue_name, item_id, Redis.random()) do
      {:ok, true} -> :ok
      _ -> Logger.error("Failed to remove malformed job #{inspect(item_id)}")
    end

    {:noreply, queue_name}
  end

  @doc false
  def handle_info(:reenqueue_dead_jobs, queue) do
    Logger.info("Looking for dead jobs for queue #{queue}")
    redis = Verk.Redis.random()

    with {:ok, node_ids} <- Queue.pending_node_ids(queue, redis),
         dead_node_ids <- dead_node_ids(node_ids, redis) do
      Enum.map(dead_node_ids, fn dead_node_id ->
        case Queue.pending(queue, dead_node_id, @max_jobs, redis) do
          {:ok, jobs} ->
            reenqueue_dead_jobs(queue, jobs, redis)

          {:error, reason} ->
            Logger.error(
              "Failed to get pending jobs from #{dead_node_id}. Reason: #{inspect(reason)}"
            )
        end
      end)
    end

    frequency = Confex.get_env(:verk, :heartbeat, 30_000)
    Process.send_after(self(), :reenqueue_dead_jobs, frequency)
    {:noreply, queue}
  end

  defp reenqueue_dead_jobs(queue, jobs, redis) do
    for {job_id, idle_time} <- jobs do
      case Queue.reenqueue_pending_job(queue, job_id, idle_time, redis) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.error(
            "Failed to enqueue jobs from dead node to queue #{queue}. Reason: #{inspect(reason)}"
          )
      end
    end
  end

  defp dead_node_ids(node_ids, redis) do
    Enum.filter(node_ids, fn node_id ->
      case Node.dead?(node_id, redis) do
        {:ok, true} -> true
        _ -> false
      end
    end)
  end

  defp format_stacktrace(stacktrace) when is_list(stacktrace) do
    stacktrace_limit =
      Confex.get_env(:verk, :failed_job_stacktrace_size, @default_stacktrace_size)

    Exception.format_stacktrace(Enum.slice(stacktrace, 0..(stacktrace_limit - 1)))
  end

  defp format_stacktrace(stacktrace), do: inspect(stacktrace)
end
