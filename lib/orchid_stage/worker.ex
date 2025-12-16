defmodule OrchidStage.Worker do
  use GenStage
  alias Orchid.Runner

  def start_link(args) do
    GenStage.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    producer = Keyword.fetch!(args, :producer)
    # :consumer means it will pull demand from producer
    {:consumer, %{producer: producer}, subscribe_to: [{producer, max_demand: 1}]}
  end

  @impl true
  def handle_events(events, _from, state) do
    Enum.each(events, fn {step, idx, params, opts} ->
      process_step(state.producer, step, idx, params, opts)
    end)

    {:noreply, [], state}
  end

  defp process_step(producer, step, idx, params, opts) do
    # We wrap everything in try/catch to ensure the Producer always gets a message
    result_msg =
      try do
        case Runner.run(step, params, opts) do
          {:ok, result} -> {:step_completed, idx, result}
          {:error, reason} -> {:step_failed, idx, reason}
        end
      catch
        kind, reason ->
          {:step_failed, idx, {kind, reason}}
      end

    GenStage.cast(producer, result_msg)
  end
end
