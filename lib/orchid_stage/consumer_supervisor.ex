defmodule OrchidStage.ConsumerSupervisor do
  use Elixir.Supervisor

  def start_link(args) do
    Elixir.Supervisor.start_link(__MODULE__, args)
  end

  @impl true
  def init(args) do
    ref = args[:ref]
    worker_num = args[:worker_num]

    # Subscribe to the specific producer for this execution reference
    producer_name = {:via, Registry, {Orchid.Registry, {ref, :producer}}}

    children =
      for i <- 1..worker_num do
        Elixir.Supervisor.child_spec(
          {OrchidStage.Worker, [producer: producer_name, id: i]},
          id: {:worker, i}
        )
      end

    Elixir.Supervisor.init(children, strategy: :one_for_one)
  end
end
