defmodule Helpers do
  def render_list(items) do
    template = """
    <%= for {name, info} <- items do %>
    * `<%= inspect(name) %>` (<%= info[:type] %>) - <%= info[:doc] %>
    <% end %>
    """

    template
    |> EEx.eval_string(items: items)
    |> String.replace(~r(\n+), "\n")
  end
end

defmodule Generate do
  require EEx

  def go(%{title: title, sections: sections}) do
    template = """
    <% import Helpers %>
    # <%= title %>

    <%= for section <- sections do %>

    ## <%= section.title %>

    <%= if section[:doc] do %><%= section.doc %><% end %>

    <%= for {event, data} <- section.events do %>
    ### `<%= event %>`

    <%= if data[:since] do %>
    *Available since v<%= data[:since] %>*.
    <% end %>

    <%= if data[:doc] do %><%= data[:doc] %><% end %>

    <%= if (data[:measurements] || []) == [] do %>
    **Measurements**: *none*
    <% else %>
    **Measurements**:
    <%= render_list(data[:measurements]) %>
    <% end %>

    <%= if (data[:metadata] || []) == [] do %>
    **Metadata**: *none*
    <% else %>
    **Metadata**:
    <%= render_list(data[:metadata]) %>
    <% end %>

    <% end %>
    <% end %>
    """

    EEx.eval_string(template, title: title, sections: sections)
  end
end

span_measurements = [
  system_time: [
    type: "`t:integer/0`",
    doc: "in `:native` time units (only for `[..., :start]` events)"
  ],
  monotonic_time: [
    type: "`t:integer/0`",
    doc: "in `:native` time units"
  ],
  duration: [
    type: "`t:integer/0`",
    doc: """
    in `:native` time units (only for `[..., :stop]` and `[..., :exception]` events)
    """
  ]
]

shared_connection_meta = [
  connection: [
    type: "`t:pid/0`",
    doc: "the PID of the connection process"
  ],
  connection_name: [
    type: "`t:String.t/0` or `nil`",
    doc: "given name of the connection or `nil` if not set"
  ],
  address: [
    type: "`t:String.t/0`",
    doc: "the address of the node the connection is connected to"
  ],
  port: [
    type: "`t::inet.port_number/0`",
    doc: "the port of the node the connection is connected to"
  ]
]

shared_cluster_meta = [
  cluster_pid: [
    type: "`t:pid/0`",
    doc: "the PID of the cluster process"
  ],
  cluster_name: [
    type: "`t:String.t/0` or `nil`",
    doc: """
    the name of the cluster executing the event, if provided
    through the `:name` option in `Xandra.Cluster.start_link/1`
    """
  ],
  host: [
    type: "`t:Xandra.Cluster.Host.t/0`",
    doc: "the host the event is related to"
  ]
]

data = %{
  title: "Telemetry Events",
  sections: [
    %{
      title: "Connection Events",
      events: [
        "[:xandra, :connected]": %{
          doc: "Executed when a connection connects to its Cassandra node.",
          metadata:
            shared_connection_meta ++
              [
                protocol_module: [
                  type: "`t:module/0`",
                  doc: "the protocol module used for the connection"
                ],
                supported_options: [
                  type: "`t:map/0`",
                  doc: """
                  Cassandra supported options (mostly useful for internal debugging)
                  """
                ]
              ]
        },
        "[:xandra, :disconnected]": %{
          doc: "Executed when a connection disconnects from its Cassandra node.",
          metadata:
            shared_connection_meta ++
              [
                reason: [
                  type: "usually a `DBConnection.ConnectionError`",
                  doc: "the reason for the disconnection"
                ]
              ]
        },
        "[:xandra, :failed_to_connect]": %{
          since: "0.18.0",
          doc: "Executed when a connection fails to connect to its Cassandra node.",
          metadata:
            shared_connection_meta ++
              [
                reason: [
                  type: "usually a `DBConnection.ConnectionError`",
                  doc: "the reason for the disconnection"
                ]
              ]
        }
      ]
    },
    %{
      title: "Query Events",
      doc: """
      The `[:xandra, :prepare_query, ...]` and `[:xandra, :execute_query, ...]` events are
      Telemetry **spans**. See
      [`telemetry:span/3`](https://hexdocs.pm/telemetry/telemetry.html#span/3). All the time
      measurements are in *native* time unit, so you need to use `System.convert_time_unit/3`
      to convert to the desired time unit.
      """,
      events: [
        "[:xandra, :prepare_query, ...]": [
          doc: "Executed before and after a query is prepared (as a Telemetry **span**).",
          measurements: span_measurements,
          metadata:
            shared_connection_meta ++
              [
                query: [
                  type: "`t:Xandra.Prepared.t/0`",
                  doc: "the query being prepared"
                ],
                extra_metadata: [
                  type: "any term",
                  doc: """
                  extra metadata provided by the `:telemetry_metadata` option
                  """
                ],
                reprepared: [
                  type: "`t:boolean/0`",
                  doc: """
                  whether the query was reprepared or not (only available for
                  `[..., :stop]` events)
                  """
                ],
                reason: [
                  type: "any term",
                  doc: """
                  if there the result of the query was an error, this is the reason
                  (only available for `[..., :stop]` events), otherwise it's the error
                  that was raised (only available for `[..., :exception]` events)
                  """
                ],
                kind: [
                  type: "`t:Exception.kind/0`",
                  doc: "exception kind (only available for `[..., :exception]` events)"
                ],
                stacktrace: [
                  type: "`t:Exception.stacktrace/0`",
                  doc: "exception stacktrace (only available for `[..., :exception]` events)"
                ]
              ]
        ],
        "[:xandra, :execute_query, ...]": [
          doc: "Executed before and after a query is executed (as a Telemetry **span**).",
          measurements: span_measurements,
          metadata:
            shared_connection_meta ++
              [
                query: [
                  type: "`t:Xandra.Simple.t/0`, `t:Xandra.Batch.t/0`, or `t:Xandra.Prepared.t/0`",
                  doc: "the query being executed"
                ],
                extra_metadata: [
                  type: "any term",
                  doc: """
                  extra metadata provided by the `:telemetry_metadata` option
                  """
                ],
                reason: [
                  type: "any term",
                  doc: """
                  if there the result of the query was an error, this is the reason
                  (only available for `[..., :stop]` events), otherwise it's the error
                  that was raised (only available for `[..., :exception]` events)
                  """
                ],
                kind: [
                  type: "`t:Exception.kind/0`",
                  doc: "exception kind (only available for `[..., :exception]` events)"
                ],
                stacktrace: [
                  type: "`t:Exception.stacktrace/0`",
                  doc: "exception stacktrace (only available for `[..., :exception]` events)"
                ]
              ]
        ],
        "[:xandra, :prepared_cache, :hit | :miss]": [
          doc: "Executed when a query is executed and the prepared cache is checked.",
          measurements: [],
          metadata:
            shared_connection_meta ++
              [
                query: [
                  type: "`t:Xandra.Prepared.t/0`",
                  doc: "the query being prepared"
                ],
                extra_metadata: [
                  type: "any term",
                  doc: """
                  extra metadata provided by the `:telemetry_metadata` option
                  """
                ]
              ]
        ]
      ]
    },
    %{
      title: "Warnings",
      events: [
        "[:xandra, :server_warnings]": [
          doc: "Executed when a query returns warnings.",
          measurements: [
            warnings: [
              type: "non-empty list of `t:String.t/0`",
              doc: "a list of warnings"
            ]
          ],
          metadata: [
            address: [
              type: "`t:String.t/0`",
              doc: "the address of the node the connection is connected to"
            ],
            port: [
              type: "`t::inet.port_number/0`",
              doc: "the port of the node the connection is connected to"
            ],
            current_keyspace: [
              type: "`t:String.t/0` or `nil`",
              doc: "the current keyspace of the connection, or `nil` if not set"
            ],
            query: [
              type: "`t:Xandra.Simple.t/0`, `t:Xandra.Batch.t/0`, or `t:Xandra.Prepared.t/0`",
              doc: "the query that caused the warnings"
            ]
          ]
        ]
      ]
    },
    %{
      title: "Cluster Events",
      doc: "Unless specified otherwise, these are available since v0.15.0.",
      events: [
        "[:xandra, :cluster, :change_event]": [
          doc: """
          Emitted when there is a change in the cluster, either as reported by Cassandra itself
          or as detected by Xandra.
          """,
          metadata:
            shared_cluster_meta ++
              [
                event_type: [
                  type: "`t:atom/0`",
                  doc: """
                  one of `:host_up` (a host went up), `:host_down` (a host went down),
                  `:host_added` (a host was added to the cluster topology), or `:host_removed`
                  (a host was removed from the cluster topology)
                  """
                ]
              ]
        ],
        "[:xandra, :cluster, :discovered_peers]": [
          since: "0.17.0",
          doc: """
          Executed when the Xandra cluster's control connection discovers peers. The peers might have been
          already discovered in the past, so you'll need to keep track of new peers if you need to.
          """,
          measurements: [
            peers: [
              type: "list of `t:Xandra.Cluster.Host.t/0`",
              doc: "the discovered peers"
            ]
          ],
          metadata: Keyword.delete(shared_cluster_meta, :host)
        ],
        "[:xandra, :cluster, :pool, :started | :restarted | :stopped]": [
          since: "0.17.0",
          doc: """
          Executed when a connection pool to a node is started, restarted, or stopped.
          """,
          metadata: shared_cluster_meta
        ],
        "[:xandra, :cluster, :control_connection, :connected]": [
          doc: """
          Emitted when the control connection for the cluster is established.
          """,
          metadata: shared_cluster_meta
        ],
        "[:xandra, :cluster, :control_connection, :disconnected | :failed_to_connect]": [
          doc: """
          Emitted when the control connection for the cluster disconnects or fails to connect.
          """,
          metadata:
            shared_cluster_meta ++
              [
                reason: [
                  type: "any term",
                  doc: "the reason for the disconnection or failure to connect"
                ]
              ]
        ]
      ]
    }
  ]
}

path = "pages/Telemetry events.md"
markdown = Generate.go(data)
File.write!(path, markdown)

IO.puts(IO.ANSI.format([:cyan, "Generated Telemetry docs", :reset, " (at #{inspect(path)})"]))
