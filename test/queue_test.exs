defmodule Verk.QueueTest do
  use ExUnit.Case, async: true
  import Verk.Queue
  alias Verk.Job

  @queue "default"
  @queue_key "verk:queue:default"

  setup do
    {:ok, pid} = Confex.get_env(:verk, :redis_url) |> Redix.start_link(name: Verk.Redis)

    Redix.command!(pid, ~w(DEL #{@queue_key}))
    ensure_group_exists!(@queue, pid)

    on_exit(fn ->
      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}
    end)

    :ok
  end

  defp ensure_group_exists!(queue, redis) do
    Redix.command!(redis, ["XGROUP", "CREATE", queue_name(queue), "verk", 0, "MKSTREAM"])
  rescue
    _ -> :ok
  end

  defp add_jobs!(queue, amount) do
    for i <- 1..amount do
      Redix.command!(Verk.Redis, ~w(XADD #{queue_name(queue)} * job job#{i}))
    end
  end

  describe "enqueue/2" do
    test "add job to the queue" do
      job = %Job{queue: @queue}
      encoded_job = Job.encode!(job)
      assert {:ok, item_id} = enqueue(job)

      assert [[^item_id, ["job", ^encoded_job]]] =
               Redix.command!(Verk.Redis, ["XRANGE", @queue_key, "-", "+"])
    end
  end

  describe "enqueue!/2" do
    test "add job to the queue" do
      job = %Job{queue: @queue}
      encoded_job = Job.encode!(job)
      item_id = enqueue!(job)

      assert [[^item_id, ["job", ^encoded_job]]] =
               Redix.command!(Verk.Redis, ["XRANGE", @queue_key, "-", "+"])
    end
  end

  describe "consume/6" do
    test "consume jobs from the queue" do
      job = %Job{queue: @queue}
      encoded_job = Job.encode!(job)
      item_id = enqueue!(job)
      {:ok, jobs} = consume(@queue, "test-123", ">", 5)
      assert jobs == [[item_id, ["job", encoded_job]]]

      pending =
        Redix.command!(Verk.Redis, ["XPENDING", @queue_key, "verk", "-", "+", 5, "test-123"])

      assert [[^item_id, "test-123", _, _]] = pending
    end
  end

  describe "pending_node_ids/1" do
    test "empty queue" do
      assert pending_node_ids(@queue) == {:ok, []}
    end

    test "non-empty queue" do
      add_jobs!(@queue, 3)

      assert pending_node_ids(@queue) == {:ok, []}
    end

    test "non-empty queue consuming jobs" do
      add_jobs!(@queue, 3)
      {:ok, _jobs} = consume(@queue, "test-123", ">", 2)

      assert pending_node_ids(@queue) == {:ok, ["test-123"]}
    end
  end

  describe "pending/3" do
    test "empty queue" do
      assert pending(@queue, "test-123", 1) == {:ok, []}
    end

    test "non-empty queue" do
      add_jobs!(@queue, 3)

      assert pending(@queue, "test-123", 1) == {:ok, []}
    end

    test "non-empty queue consuming jobs" do
      [job1, job2, _job3] = add_jobs!(@queue, 3)
      {:ok, _} = consume(@queue, "test-123", ">", 2)

      assert {:ok, pending_jobs} = pending(@queue, "test-123", 2)
      assert [{^job1, _}, {^job2, _}] = pending_jobs
    end
  end

  describe "count_pending/1" do
    test "empty queue" do
      assert count_pending(@queue) == {:ok, 0}
    end

    test "non-empty queue" do
      add_jobs!(@queue, 3)

      assert count_pending(@queue) == {:ok, 0}
    end

    test "non-empty queue consuming jobs" do
      add_jobs!(@queue, 3)
      {:ok, _jobs} = consume(@queue, "test-123", ">", 2)

      assert count_pending(@queue) == {:ok, 2}
    end
  end

  describe "count_pending!/1" do
    test "empty queue" do
      assert count_pending!(@queue) == 0
    end

    test "non-empty queue" do
      add_jobs!(@queue, 3)

      assert count_pending!(@queue) == 0
    end

    test "non-empty queue consuming jobs" do
      add_jobs!(@queue, 3)
      {:ok, _jobs} = consume(@queue, "test-123", ">", 2)

      assert count_pending!(@queue) == 2
    end
  end

  describe "count/1" do
    test "empty queue" do
      assert count(@queue) == {:ok, 0}
    end

    test "non-empty queue" do
      add_jobs!(@queue, 3)

      assert count(@queue) == {:ok, 3}
    end
  end

  describe "count!/1" do
    test "non-empty queue" do
      add_jobs!(@queue, 3)

      assert count!(@queue) == 3
    end

    test "empty queue" do
      assert count!(@queue) == 0
    end
  end

  describe "clear/1" do
    test "clear queue" do
      assert clear(@queue) == {:ok, true}
      assert clear(@queue) == {:ok, false}

      add_jobs!(@queue, 3)

      assert clear(@queue) == {:ok, true}

      assert Redix.command!(Verk.Redis, ~w(GET #{@queue_key})) == nil
    end
  end

  describe "clear!/1" do
    test "clear!" do
      assert clear!(@queue) == true
      assert clear!(@queue) == false

      add_jobs!(@queue, 3)

      assert clear!(@queue) == true

      assert Redix.command!(Verk.Redis, ~w(GET #{@queue_key})) == nil
    end
  end

  describe "range/1" do
    test "with items" do
      job = %Job{class: "Class", args: []}
      json = Job.encode!(job)
      item_id = Redix.command!(Verk.Redis, ~w(XADD #{@queue_key} * job #{json}))

      assert range(@queue) == {:ok, [%{job | original_json: json, item_id: item_id}]}
    end

    test "with no items" do
      assert range(@queue) == {:ok, []}
    end
  end

  describe "range!/1" do
    test "with items" do
      job = %Job{class: "Class", args: []}
      json = Job.encode!(job)
      item_id = Redix.command!(Verk.Redis, ~w(XADD #{@queue_key} * job #{json}))

      assert range!(@queue) == [%{job | original_json: json, item_id: item_id}]
    end

    test "with no items" do
      assert range!(@queue) == []
    end
  end

  describe "delete_job/2" do
    test "no job inside the queue" do
      job = %Job{item_id: "123"}
      assert delete_job(@queue, job) == {:ok, false}
      assert delete_job(@queue, "123") == {:ok, false}
    end

    test "job with item_id" do
      job = %Job{class: "Class", args: []}
      json = Job.encode!(job)

      item_id = Redix.command!(Verk.Redis, ~w(XADD #{@queue_key} * job #{json}))

      job = %{job | original_json: json, item_id: item_id}

      assert delete_job(@queue, job) == {:ok, true}
    end

    test "item_id" do
      json = %Job{class: "Class", args: []} |> Job.encode!()

      item_id = Redix.command!(Verk.Redis, ~w(XADD #{@queue_key} * job #{json}))

      assert delete_job(@queue, item_id) == {:ok, true}
    end
  end

  describe "delete_job!/2" do
    test "no job inside the queue" do
      assert delete_job!(@queue, %Job{item_id: "123"}) == false
      assert delete_job!(@queue, "123") == false
    end

    test "job with item_id" do
      job = %Job{class: "Class", args: []}
      json = Job.encode!(job)

      item_id = Redix.command!(Verk.Redis, ~w(XADD #{@queue_key} * job #{json}))

      job = %{job | original_json: json, item_id: item_id}

      assert delete_job!(@queue, job) == true
    end

    test "item_id" do
      json = %Job{class: "Class", args: []} |> Job.encode!()

      item_id = Redix.command!(Verk.Redis, ~w(XADD #{@queue_key} * job #{json}))

      assert delete_job!(@queue, item_id) == true
    end
  end
end
