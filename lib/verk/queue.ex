defmodule Verk.Queue do
  @moduledoc """
  This module interacts with a queue
  """
  alias Verk.Job
  import Verk.Dsl

  @external_resource "#{:code.priv_dir(:verk)}/reenqueue_pending_job.lua"
  @reenqueue_pending_job_script_sha Verk.Scripts.sha("reenqueue_pending_job")

  @doc false
  def queue_name(queue) do
    "verk:queue:#{queue}"
  end

  @doc """
  Enqueue job
  """
  @spec enqueue(Job.t(), GenServer.server()) :: {:ok, binary} | {:error, atom | Redix.Error.t()}
  def enqueue(job, redis \\ Verk.Redis) do
    encoded_job = Job.encode!(job)
    Redix.command(redis, ["XADD", queue_name(job.queue), "*", "job", encoded_job])
  end

  @doc """
  Enqueue job, raising if there's an error
  """
  @spec enqueue!(Job.t(), GenServer.server()) :: binary
  def enqueue!(job, redis \\ Verk.Redis) do
    bangify(enqueue(job, redis))
  end

  @doc """
  Consume max of `count` jobs from `queue` identifying as `consumer_id`
  """
  @spec consume(binary, binary, binary, pos_integer, pos_integer, GenServer.server()) ::
          {:ok, term} | {:error, Redix.Error.t()}
  def consume(queue, consumer_id, last_id, count, timeout \\ 30_000, redis \\ Verk.Redis) do
    command = [
      "XREADGROUP",
      "GROUP",
      "verk",
      consumer_id,
      "COUNT",
      count,
      "BLOCK",
      timeout,
      "STREAMS",
      queue_name(queue),
      last_id
    ]

    case Redix.command(redis, command, timeout: timeout + 5000) do
      {:ok, [[_, jobs]]} -> {:ok, jobs}
      result -> result
    end
  end

  @doc """
  List node ids that have pending jobs
  """
  def pending_node_ids(queue, redis \\ Verk.Redis) do
    case Redix.command(redis, ["XPENDING", queue_name(queue), "verk"]) do
      {:ok, [_, _, _, nil]} -> {:ok, []}
      {:ok, [_, _, _, nodes]} -> {:ok, Enum.map(nodes, &List.first(&1))}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  List pending jobs from a queue being consumed by a node_id
  It returns a list of tuples like this: {job_id, idle_time}
  """
  @spec pending(binary, binary, GenServer.server()) ::
          {:ok, [{binary, non_neg_integer}]} | {:error, atom | Redix.Error.t()}
  def pending(queue, node_id, count, redis \\ Verk.Redis) do
    case Redix.command(redis, ["XPENDING", queue_name(queue), "verk", "-", "+", count, node_id]) do
      {:ok, result} ->
        {:ok, Enum.map(result, fn [job_id, _, idle_time, _] -> {job_id, idle_time} end)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def reenqueue_pending_job(queue, job_id, idle_time, redis \\ Verk.Redis) do
    case Redix.command(redis, [
           "EVALSHA",
           @reenqueue_pending_job_script_sha,
           1,
           queue_name(queue),
           "verk",
           job_id,
           idle_time
         ]) do
      {:ok, _} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Counts how many jobs are enqueued
  """
  @spec count(binary) :: {:ok, integer} | {:error, atom | Redix.Error.t()}
  def count(queue) do
    commands = [
      ["MULTI"],
      ["XLEN", queue_name(queue)],
      ["XPENDING", queue_name(queue), "verk"],
      ["EXEC"]
    ]

    case Redix.pipeline(Verk.Redis, commands) do
      {:ok, [_, _, _, [total, [pending | _]]]} -> {:ok, total - pending}
      error -> error
    end
  end

  @doc """
  Counts how many jobs are enqueued on a queue, raising if there's an error
  """
  @spec count!(binary) :: integer
  def count!(queue) do
    bangify(count(queue))
  end

  @doc """
  Counts how many jobs are pending to be ack'd
  """
  @spec count_pending(binary) :: {:ok, integer} | {:error, atom | Redix.Error.t()}
  def count_pending(queue) do
    case Redix.command(Verk.Redis, ["XPENDING", queue_name(queue), "verk"]) do
      {:ok, [pending | _]} -> {:ok, pending}
      error -> error
    end
  end

  @doc """
  Counts how many jobs are pending to be ack'd, raising if there's an error
  """
  @spec count_pending!(binary) :: integer
  def count_pending!(queue) do
    bangify(count_pending(queue))
  end

  @doc """
  Clears the `queue`

  It will return `{:ok, true}` if the `queue` was cleared and `{:ok, false}` otherwise

  An error tuple may be returned if Redis failed
  """
  @spec clear(binary) :: {:ok, boolean} | {:error, Redix.Error.t()}
  def clear(queue) do
    case Redix.command(Verk.Redis, ["DEL", queue_name(queue)]) do
      {:ok, 0} -> {:ok, false}
      {:ok, 1} -> {:ok, true}
      {:error, error} -> {:error, error}
    end
  end

  @doc """
  Clears the `queue`, raising if there's an error

  It will return `true` if the `queue` was cleared and `false` otherwise
  """
  @spec clear!(binary) :: boolean
  def clear!(queue) do
    bangify(clear(queue))
  end

  @doc """
  Lists enqueued jobs from `start` to `stop`
  """
  @spec range(binary, binary, binary, non_neg_integer) :: {:ok, [Verk.Job.T]} | {:error, Redix.Error.t()}
  def range(queue, start \\ "-", stop \\ "+", count \\ 25) do
    case Redix.command(Verk.Redis, ["XRANGE", queue_name(queue), start, stop, "COUNT", count]) do
      {:ok, jobs} ->
        {:ok, for([item_id, ["job", job]] <- jobs, do: Job.decode!(job, item_id))}

      {:error, error} ->
        {:error, error}
    end
  end

  @doc """
  Lists enqueued jobs from `start` to `stop`, raising if there's an error
  """
  @spec range!(binary, binary, binary, non_neg_integer) :: [Verk.Job.T]
  def range!(queue, start \\ "-", stop \\ "+", count \\ 25) do
    bangify(range(queue, start, stop))
  end

  @doc """
  Deletes the job from the `queue`

  It returns `{:ok, true}` if the job was found and deleted
  Otherwise it returns `{:ok, false}`

  An error tuple may be returned if Redis failed
  """
  @spec delete_job(binary, Job.t() | binary, GenServer.server()) ::
          {:ok, boolean} | {:error, Redix.Error.t()}
  def delete_job(queue, item_id, redis \\ Verk.Redis)

  def delete_job(queue, item_id, redis) when is_binary(item_id) do
    case Redix.pipeline(redis, [
           ["XACK", queue_name(queue), "verk", item_id],
           ["XDEL", queue_name(queue), item_id]
         ]) do
      {:ok, [_, 1]} -> {:ok, true}
      {:ok, _} -> {:ok, false}
      {:error, error} -> {:error, error}
    end
  end

  def delete_job(queue, job, redis), do: delete_job(queue, job.item_id, redis)

  @doc """
  Delete job from the `queue`, raising if there's an error

  It returns `true` if the job was found and delete
  Otherwise it returns `false`

  An error will be raised if Redis failed
  """
  @spec delete_job!(binary, Job.t() | binary, GenServer.Sever) :: boolean
  def delete_job!(queue, job, redis \\ Verk.Redis) do
    bangify(delete_job(queue, job, redis))
  end
end
