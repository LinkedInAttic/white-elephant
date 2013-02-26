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

App.TableView = Em.View.extend(
  templateName: "table"

  renderTable: (->
    console?.log "Rendering table"

    $table = $("#table")
    $table.html('')

    data = this.get("data")

    unless data
      console?.log "No data"
      return

    if data.users.length > 0
      # need to render ourselves instead of with ember because the metamorphs
      # don't play well with tablesorter and pager
      html = Mustache.render("""
      <table class="table tablesorter">
      <thead>
        <tr>
          <th>User</th>
          {{#times}}
            <th>{{.}}</th>
          {{/times}}
          <th>total</th>
        </tr>      
      </thead>
      <tbody>
        {{#users}}
        <tr>
        <td>{{name}}</td>
        {{#data}}
          <td>{{.}}</td>
        {{/data}}
        <td>{{total}}</td>
        </tr>
        {{/users}}
      </tbody>
      </table>
      """,data)
      
      $table.append(html)

      last_column = data.times.length+1

      $("#table .table").tablesorter(
                          sortList: [[last_column,1]]
                        )
                        .tablesorterPager(
                          container: $("#pager")
                          positionFixed: false
                        ); 

      $("#pager").show()
    else
      console?.log "Table will not be shown, no users selected"
      $("#pager").hide()

    console?.log "Rendered table"

  ).observes("data")

  data: (->
    console?.log "Getting data"

    result = {}

    data = this.get("controller").get("usageData")
    type = this.get("controller").get("selectedType")

    unless data
      console?.log "Missing usage data"
      return

    unless data.times
      console?.log "No times"
      return

    unless data.users
      console?.log "No users"
      return

    unit = this.get("controller").get("selectedUnit")

    times = []
    _(_(data.times).last(7)).each((time) ->
      switch unit
        when "DAYS"
          times.push(moment(time).format("MMM Do"))
        when "WEEKS"
          times.push(moment(time).format("MMM Do"))
        when "MONTHS"
          times.push(moment(time).format("MMMM"))
        else
          console?.log "Unexpected: #{unit}"
          return
    )

    result.times = times

    unless data
      console?.log "Missing usage data"
      return

    unless type
      console?.log "Missing type"
      return

    is_minutes = switch type
      when "cpuTotal", "minutesTotal", "minutesReduce", "minutesMap", "minutesExcessTotal", "minutesExcessReduce", "minutesExcessMap", "minutesSuccess", "minutesFailed", "minutesKilled"
        true
      else false

    users = data.users.slice(0,data.users.length)

    final_users = []

    _(users).each((user_data) ->
      new_user_data =
        name: user_data.user
        data: _(_(user_data.data).last(7)).map((val)->
          if is_minutes
            val = val/60.0
            Math.round(val*10.0)/10.0
          else val
        )

      new_user_data.total = _(user_data.data).reduce((memo,num) -> 
        memo+num
      , 0)

      if is_minutes
        new_user_data.total = Math.round(new_user_data.total*10.0)/10.0

      final_users.push(new_user_data)
    )

    result.users = final_users

    console?.log "Got data"

    result
  ).property("controller.usageData")
)