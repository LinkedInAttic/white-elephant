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

App.UsageQueryView = Em.View.extend(
  templateName: "usage_query"

  units:[{label:"Daily", value:"DAYS"},
     {label:"Weekly", value:"WEEKS"},
     {label:"Monthly", value:"MONTHS"},
     {label:"Quarterly", value:"QUARTERS"}]

  types:[{label:"Total Hours", value:"minutesTotal"},
         {label:"Reduce Hours", value:"minutesReduce"},
         {label:"Map Hours", value:"minutesMap"},
         {label:"Total Hours (excess)", value:"minutesExcessTotal"},
         {label:"Reduce Hours (excess)", value:"minutesExcessReduce"},
         {label:"Map Hours (excess)", value:"minutesExcessMap"},
         {label:"CPU Hours", value:"cpuTotal"},
         {label:"Successful Hours", value:"minutesSuccess"},
         {label:"Failed Hours", value:"minutesFailed"},
         {label:"Killed Hours", value:"minutesKilled"},
         {label:"Tasks", value:"totalStarted"},
         {label:"Maps", value:"mapStarted"},
         {label:"Reduces", value:"reduceStarted"},
         {label:"Successful", value:"successFinished"},
         {label:"Failed", value:"failedFinished"},
         {label:"Killed", value:"killedFinished"},
         {label:"Reduce Shuffle Bytes", value:"reduceShuffleBytes"}]

  didInsertElement: ->
    $(".users").multiselect().multiselectfilter()
    $(".users").bind("multiselectclick checkall uncheckall",(event,ui)=>
      this.userSelectionChanged()
    )

    $(".users").bind("multiselectcheckall",(event,ui)=>
      this.userSelectionChanged()
    )

    $(".users").bind("multiselectuncheckall",(event,ui)=>
      this.userSelectionChanged()
    )

    $(".users").multiselect("disable")

  inProgressChanged: (->
    in_progress = this.get("controller").get("isInProgress")>0
    if in_progress
      $(".users").multiselect("disable")
    else
      $(".users").multiselect("enable")
  ).observes("controller.inProgressCount")

  userSelectionChanged: ->
    checked = $(".users").multiselect("getChecked")
    users = []
    _.each(checked,(cb) ->
      user_name = $(cb).val()
      users.push(user_name)
    )      
    this.get("controller").set("selectedUsers",users)

  usersChanged: (->
    $users = $(".users")

    data = {users:this.get("controller").get("users")}

    html = Mustache.render("""
      {{#users}}
        <option>{{name}}</option>
      {{/users}}
    """,data)

    $users.html(html)

    _.delay(->
      $users.multiselect("refresh").multiselect("checkAll")
    )
    
  ).observes("controller.users")
)