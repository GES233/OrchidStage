defmodule OrchidStage.Producer do
  use GenStage
  alias Orchid.Scheduler

  defstruct [
    :context,
    :queue,
    :demand,
    :retry_limit,
    :retry_counts, # %{step_idx => count}
    :running_refs  # %{ref => step_idx} for future monitoring if needed
  ]

  def start_link(opts) do
    name = Keyword.get(opts, :name)
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    state = %__MODULE__{
      context: Keyword.fetch!(opts, :context),
      queue: :queue.new(),
      demand: 0,
      retry_limit: Keyword.get(opts, :retry_limit, 0),
      retry_counts: %{},
      running_refs: %{}
    }

    # Dispatch initially in case no demand comes (unlikely for GenStage but good practice)
    # Actually, GenStage waits for demand. So we just return sync.
    {:producer, state}
  end

  @impl true
  def handle_demand(incoming_demand, state) do
    new_state = %{state | demand: state.demand + incoming_demand}
    dispatch_events(new_state)
  end

  # Received result from Worker
  @impl true
  def handle_cast({:step_completed, step_idx, output}, state) do
    # 1. Update Context
    new_ctx = Scheduler.merge_result(state.context, step_idx, output)

    # 2. Check if Done
    if Scheduler.done?(new_ctx) do
      {:stop, {:shutdown, {:ok, Scheduler.get_results(new_ctx)}}, state}
    else
      # 3. Continue dispatching
      state
      |> Map.put(:context, new_ctx)
      |> dispatch_events()
    end
  end

  # Received failure from Worker
  @impl true
  def handle_cast({:step_failed, step_idx, reason}, state) do
    current_retries = Map.get(state.retry_counts, step_idx, 0)

    if current_retries < state.retry_limit do
      # RETRY LOGIC
      # We need to make the step "ready" again.
      # Scheduler.merge_result removes it from running/pending, but here we failed.
      # We need a hack: Manually remove it from 'running_steps' in context so next_ready_steps picks it up.

      # Log warning (or define a Logger)
      # Logger.warning("Step #{step_idx} failed. Retrying (#{current_retries + 1}/#{state.retry_limit})...")

      failed_ctx = state.context
      # Force removal from running set, effectively putting it back to pending state (since it wasn't removed from pending list in ctx yet)
      reset_ctx = %{failed_ctx | running_steps: MapSet.delete(failed_ctx.running_steps, step_idx)}

      new_state = %{state |
        context: reset_ctx,
        retry_counts: Map.put(state.retry_counts, step_idx, current_retries + 1)
      }

      dispatch_events(new_state)
    else
      # FAIL
      {:stop, {:shutdown, {:error, {:step_failed_exhausted, step_idx, reason}}}, state}
    end
  end

  defp dispatch_events(state) do
    # 1. Get ready steps from Scheduler
    ready_steps = Scheduler.next_ready_steps(state.context)

    # 2. Calculate how many we can send
    # We limit by demand, but also purely by what is ready.
    # GenStage usually buffers, but we want tight control, so we only emit what we have.
    count_to_send = min(state.demand, length(ready_steps))

    {steps_to_dispatch, _remaining_ready} = Enum.split(ready_steps, count_to_send)

    # 3. Mark them as running in Scheduler immediately
    indices = Enum.map(steps_to_dispatch, fn {_, idx} -> idx end)
    new_ctx = Scheduler.mark_running(state.context, indices)

    # 4. Prepare events payload
    events = Enum.map(steps_to_dispatch, fn {step, idx} ->
      # Optimization: Only send necessary inputs, not the whole params map if possible.
      # But Runner.run expects params map. Let's send it all for now or optimize later.
      # To avoid huge message passing, one should use Orchid.Repo/ETS.
      {step, idx, new_ctx.params, new_ctx.recipe.opts}
    end)

    new_demand = state.demand - count_to_send

    # If we have no running steps and no ready steps, but not done => Deadlock
    if count_to_send == 0 and MapSet.size(new_ctx.running_steps) == 0 and not Scheduler.done?(new_ctx) do
       {:stop, {:shutdown, {:error, :deadlock_detected}}, state}
    else
       {:noreply, events, %{state | context: new_ctx, demand: new_demand}}
    end
  end
end
