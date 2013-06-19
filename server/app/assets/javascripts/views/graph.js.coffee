# Copyright 2012 LinkedIn, Inc

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

App = window.App

App.GraphView = Em.View.extend(
  templateName: "graph"

  renderGraph: (->
    console?.log "Rendering graph"

    $("#chart").html('')
    $("#legend").html('')
    $("#y_axis").html('')

    series = this.get("series")

    $('#slider').hide()
    $("#chart-container").hide()

    unless series
      console?.log "No series"
      return

    if series.length == 0
      console?.log "Series is empty"
      return

    $('#slider').show()      
    $("#chart-container").show()

    chart_max = this.get('controller').get('chart_max')
    chart_min = this.get('controller').get('chart_min')
    y_label = this.get('y_label')

    if chart_max
      chart_max = parseInt(chart_max)

    if chart_min
      chart_min = parseInt(chart_min)

    chart_min = null unless chart_min >= 0
    chart_max = null unless chart_max > 0

    graph = new Rickshaw.Graph(
        element: document.querySelector("#chart")
        renderer: 'area'
        stroke: true
        height: 400
        max: chart_max
        min: chart_min
        series: series                 
    );
         
    timeFixture = new Rickshaw.Fixtures.LocalTime()

    xAxis = new Rickshaw.Graph.Axis.Time(
      graph: graph
      timeFixture: timeFixture
    );

    slider = new Rickshaw.Graph.RangeSlider({
      graph: graph,
      element: $('#slider')
    });

    graph.render();

    legend = new Rickshaw.Graph.Legend(
      graph: graph,
      element: document.querySelector('#legend')
    );

    shelving = new Rickshaw.Graph.Behavior.Series.Toggle(
      graph: graph,
      legend: legend
    );

    order = new Rickshaw.Graph.Behavior.Series.Order(
      graph: graph,
      legend: legend
    );

    highlighter = new Rickshaw.Graph.Behavior.Series.Highlight(
      graph: graph,
      legend: legend
    );

    yAxis = new Rickshaw.Graph.Axis.Y(
      graph: graph
      orientation: 'left'
      tickFormat: Rickshaw.Fixtures.Number.formatKMBT
      element: document.getElementById('y_axis')
      label: y_label
    );

    yAxis.render();

    unit = this.get('controller').get('selectedUnit')

    xFormatter = switch unit
      when "QUARTERS" then (x) -> 
        x = moment(new Date(x*1000))
        quarter = "Q" + (1 + Math.floor(x.month()/3))
        quarter + " " + x.format("YYYY")
      when "MONTHS" then (x) ->
        x = moment(new Date(x*1000))
        x.format("MMM YYYY")
      when "DAYS", "WEEKS" then (x) ->
        x = moment(new Date(x*1000))
        x.format("ddd MMM Do YYYY")
      else (x) -> new Date(x*1000).toDateString()

    hoverDetail = new Rickshaw.Graph.HoverDetail(
      graph: graph
      xFormatter: xFormatter
    );
  ).observes("series","controller.chart_max","controller.chart_min")
  
  y_label: (->
    type = this.get("controller").get("selectedType")

    result = switch type
      when "cpuTotal", "minutesTotal", "minutesReduce", "minutesMap", "minutesExcessTotal", "minutesExcessReduce", "minutesExcessMap", "minutesSuccess", "minutesFailed", "minutesKilled"
        "Hours"
      when "totalStarted", "mapStarted", "reduceStarted", "successFinished", "failedFinished", "killedFinished"
        "Tasks"
      when "reduceShuffleBytes"
        "Bytes"

    result
  ).property("controller.selectedType")

  series: (->
    console?.log "Getting series"

    series = []

    controller = this.get("controller")

    data = controller.get("usageData")
    type = controller.get("selectedType")

    unless data
      console?.log "Missing usage data"
      return

    unless data.times and data.times.length > 0
      console?.log "No times"
      return

    is_minutes = switch type
      when "cpuTotal", "minutesTotal", "minutesReduce", "minutesMap", "minutesExcessTotal", "minutesExcessReduce", "minutesExcessMap", "minutesSuccess", "minutesFailed", "minutesKilled"
        true
      else false

    times = data.times
    users = data.users.slice(0,data.users.length)

    if users.length == 0
      console?.log "no users"
      return series

    other_name = null
    max_graph = this.get('controller').get('maxUsersToGraph')

    _(users).each((user) ->
      user.total = _(user.data).reduce((memo,d) ->
        memo+d
      )
    )

    # sort so heaviest users are first
    users = _(users).sortBy((user) -> -user.total)

    aggregate_selected = this.get("controller").get("aggregateSelectedChecked")

    console?.log "aggregate_selected: #{aggregate_selected}"

    # aggregate user data when there are too many to graph
    if (aggregate_selected and users.length > 0) || users.length > max_graph

      if aggregate_selected and users.length > 0
        users_to_aggregate = users
        users = []
      else
        console?.log "Got #{users.length} users, must truncate to #{max_graph}"

        # assume heaviest users are first, only take the first n
        users_to_aggregate = users.splice(max_graph,users.length-max_graph)

      console?.log "Aggregating #{users_to_aggregate.length} users"
      num_aggregated_users = users_to_aggregate.length

      aggregated_data = []

      _(users_to_aggregate).each((user) ->
        i = 0
        _(user.data).each((measurement) ->
          aggregated_data[i] ||= 0
          aggregated_data[i] += measurement
          i++
        )
      )

      other_name = "#{num_aggregated_users} users"

      users.push(
        user: other_name
        data: aggregated_data
        disabled :false
      )

    if data.users_aggregated and data.num_aggregated_users > 0
      should_disable = users.length > 0
      num_aggregated_users = data.num_aggregated_users
      aggregated_data = data.users_aggregated
      other_name = if users.length > 0
        "#{num_aggregated_users} more users"
      else
        "#{num_aggregated_users} users"
      users.push(
        user: other_name
        data: aggregated_data
        disabled: should_disable
      )

    palette = new Rickshaw.Color.Palette(
      scheme: 'spectrum14'
    )

    _.each(users, (user_data) ->       
      series_data = []
      i = 0

      _.each(user_data.data, (val) -> 
        if is_minutes
          # convert to hours
          series_data.push(
            x: times[i]/1000.0
            y: val/60.0
          )
        else
          series_data.push(
            x: times[i]/1000.0
            y: val
          )
        i++;
      )

      series.push(
        color: palette.color()
        name: user_data.user
        data: series_data
        disabled: user_data.disabled
      )
    )

    series.reverse()

    series
  ).property("controller.usageData","controller.maxUsersToGraph","controller.aggregateSelectedChecked")
)