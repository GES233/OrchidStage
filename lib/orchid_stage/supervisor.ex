defmodule OrchidStage.Supervisor do
  use Elixir.Supervisor

  def start_link(init_arg) do
    Elixir.Supervisor.start_link(__MODULE__, init_arg, name: via_name(init_arg[:ref]))
  end

  @impl true
  def init(args) do
    ref = args[:ref]
    ctx = args[:context]
    opts = args[:opts]

    producer_opts = [
      context: ctx,
      retry_limit: Keyword.get(opts, :retry, 0),
      name: {:via, Registry, {Orchid.Registry, {ref, :producer}}}
    ]

    worker_num = Keyword.get(opts, :worker_num, System.schedulers_online())

    children = [
      {OrchidStage.Producer, producer_opts},
      {OrchidStage.ConsumerSupervisor, [ref: ref, worker_num: worker_num]}
    ]

    Elixir.Supervisor.init(children, strategy: :one_for_all)
  end

  defp via_name(ref), do: {:via, Registry, {Orchid.Registry, {ref, :sup}}}
end
