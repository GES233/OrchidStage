defmodule OrchidStage do
  @moduledoc """
  A robust, concurrent executor based on GenStage.

  It implements a Producer-Consumer model where:
  - **Producer**: Holds the Scheduler Context, dispatches steps, and handles retries.
  - **Consumer (Worker)**: Executes steps and sends results back to Producer.

  ## Options
  * `:worker_num` - Number of concurrent workers (default: System.schedulers_online()).
  * `:retry` - Max retry attempts for a failed step (default: 0).
  """
  @behaviour Orchid.Executor

  # --- The Facade Implementation ---

  @impl true
  def execute(ctx, opts) do
    # 0. Ensure Registry exists (You might want to put this in your Application.ex)
    ensure_registry()

    # 1. Create a unique reference for this execution run
    ref = make_ref()

    # Catch exit signal
    Process.flag(:trap_exit, true)

    # 2. Start the ephemeral supervisor
    # We link it because if the executor process dies, the whole tree should die.
    {:ok, sup_pid} = OrchidStage.Supervisor.start_link(ref: ref, context: ctx, opts: opts)

    # 3. Find the Producer and Monitor it
    # The Producer will exit with a specific reason when the job is done.
    [{_, producer_pid, _, _} | _] =
      Elixir.Supervisor.which_children(sup_pid)
      |> Enum.filter(fn {id, _, _, _} -> id == OrchidStage.Producer end)

    monitor_ref = Process.monitor(producer_pid)

    # 4. Wait for the result
    wait_for_completion(sup_pid, monitor_ref)
  end

  defp wait_for_completion(sup_pid, monitor_ref) do
    receive do
      {:DOWN, ^monitor_ref, :process, _pid, reason} ->
        # Stop the supervisor (cleanup)
        try do
          Process.exit(sup_pid, :shutdown)
        catch
          _, _ -> :ok
        end

        receive do
          {:EXIT, ^sup_pid, _} -> :ok
        after
          0 -> :ok
        end

        case reason do
          {:shutdown, {:ok, results}} -> {:ok, results}
          {:shutdown, {:error, error}} -> {:error, error}
          # If Producer crashes unexpectedly
          other -> {:error, {:executor_crashed, other}}
        end

      {:EXIT, ^sup_pid, reason} ->
        {:error, {:supervisor_crashed, reason}}
    end
  end

  defp ensure_registry do
    if Process.whereis(Orchid.Registry) == nil do
      Registry.start_link(keys: :unique, name: Orchid.Registry)
    end
  end
end
