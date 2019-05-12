defmodule Verk.QueueConsumer do
  @moduledoc """
  QueueConsumer interacts with redis to dequeue jobs from the specified queue.
  """

  use GenServer
  require Logger
  alias Verk.Queue

  @max_jobs 100
  @max_timeout 30_000

  defmodule State do
    @moduledoc false
    defstruct [:queue, :workers_manager, :redis, :node_id, :demand, :last_id]
  end

  def reset(queue_consumer, n) do
    GenServer.cast(queue_consumer, {:reset, n})
  end

  def ask(queue_consumer, n) do
    GenServer.cast(queue_consumer, {:ask, n})
  end

  @doc false
  def start_link(queue_name, workers_manager) do
    GenServer.start_link(__MODULE__, [queue_name, workers_manager])
  end

  def init([queue, workers_manager]) do
    node_id = Confex.fetch_env!(:verk, :local_node_id)
    {:ok, redis} = Redix.start_link(Confex.get_env(:verk, :redis_url))

    state = %State{
      queue: queue,
      workers_manager: workers_manager,
      redis: redis,
      node_id: node_id,
      demand: 0,
      last_id: 0
    }

    ensure_group_exists!(queue, redis)

    Logger.info("Queue Consumer started for queue #{queue}")
    {:ok, state}
  end

  defp ensure_group_exists!(queue, redis) do
    Redix.command(redis, ["XGROUP", "CREATE", Queue.queue_name(queue), "verk", 0, "MKSTREAM"])
  rescue
    _ -> :ok
  end

  def handle_cast({:reset, new_demand}, state) do
    Logger.info "#{new_demand} reset!"
    if new_demand != 0, do: send(self(), :consume)
    {:noreply, %{state | demand: new_demand}}
  end

  def handle_cast({:ask, new_demand}, state) do
    Logger.info "#{new_demand} ask!"
    if state.demand == 0, do: send(self(), :consume)
    {:noreply, %{state | demand: state.demand + new_demand}}
  end

  def handle_info(:consume, state = %State{last_id: ">"}) do
    case consume(state) do
      {:ok, nil} ->
        send(self(), :consume)
        {:noreply, state}

      {:ok, jobs} ->
        send(state.workers_manager, {:jobs, jobs})

        new_demand = state.demand - length(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | demand: new_demand}}

      result ->
        Logger.error("Error while consuming jobs. Result: #{inspect(result)}")
        send(self(), :consume)
        {:noreply, state}
    end
  end

  def handle_info(:consume, state) do
    case consume(state) do
      {:ok, nil} ->
        send(self(), :consume)
        {:noreply, state}

      {:ok, []} ->
        send(self(), :consume)
        {:noreply, %{state | last_id: ">"}}

      {:ok, jobs} ->
        send(state.workers_manager, {:jobs, jobs})

        [last_id, _] = List.last(jobs)

        new_demand = state.demand - Enum.count(jobs)
        if new_demand > 0, do: send(self(), :consume)
        {:noreply, %{state | last_id: last_id, demand: new_demand}}

      result ->
        Logger.error("Error while consuming jobs. Result: #{inspect(result)}")
        send(self(), :consume)
        {:noreply, state}
    end
  end

  defp consume(state) do
    Logger.info("Consuming. Demand: #{state.demand}. Last id: #{state.last_id}")

    Queue.consume(
      state.queue,
      state.node_id,
      state.last_id,
      min(state.demand, @max_jobs),
      @max_timeout,
      state.redis
    )
  end
end
