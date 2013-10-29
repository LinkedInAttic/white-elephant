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

$LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__)))

require 'environment.rb'

helpers do
  def url_for_asset(asset_name)
    asset = $sprockets[asset_name]
    raise "#{asset_name} not found" unless asset
    # include digest to prevent caching stale assets
    url_for "/assets/" + asset.digest_path
  end
end

get "/" do
  erb :home
end

get "/api/users" do
  content_type :json
  cluster = params[:cluster]
  halt 500 unless cluster
  UsageData.fetch_users(cluster).to_json
end

get "/api/clusters" do
  content_type :json
  UsageData.fetch_clusters.to_json
end

get "/api/usage" do
  get_usage(params)
end

post "/api/usage" do
  parsed = JSON.parse request.body.read
  params = {}
  parsed.each do |k,v|
    params[k.to_sym] = v
  end 
  get_usage(params)
end

helpers do 
  def get_usage(params)
    content_type :json

    date_start = params[:start] || (halt 500)
    date_end = params[:end] || (halt 500)
    unit = params[:unit] || (halt 500)
    zone = params[:zone] || (halt 500)
    users = params[:user] || (halt 500)
    users_to_aggregate = params[:users_to_aggregate] || (halt 500)
    cluster = params[:cluster] || (halt 500)
    type = (params[:type] && params[:type].to_sym) || (halt 500)

    time = {
      :start => date_start.to_i,
      :end => date_end.to_i,
      :unit => unit,
      :zone => zone
    }

    users = users.split(",").uniq
    users_to_aggregate = users_to_aggregate.split(",").uniq

    return_val = {}

    if users.size > 0
      result = UsageData.fetch_per_user_data(cluster,users,time,type)

      return_val[:times] = result[:times]
      return_val[:users] = result[:users]
    else
      return_val[:users] = []
    end

    return_val[:users_aggregated] = []
    return_val[:num_aggregated_users] = 0

    if users_to_aggregate.size > 0    
      result = UsageData.fetch_aggregated_data(cluster,users_to_aggregate,time,type)
      
      return_val[:users_aggregated] = result[:data]
      return_val[:num_aggregated_users] = users_to_aggregate.size

      # get times from first available, they're all the same
      return_val[:times] ||= result[:times]
    end

    return_val[:cluster] = cluster

    return_val.to_json
  end
end

get "/api/table" do

  headers "Content-Disposition" => "attachment;filename=table.csv",
          "Content-Type" => "application/octet-stream"

  date_start = params[:start] || (halt 500)
  date_end = params[:end] || (halt 500)
  unit = params[:unit] || (halt 500)
  zone = params[:zone] || (halt 500)
  users = params[:user] || (halt 500)
  cluster = params[:cluster] || (halt 500)
  type = (params[:type] && params[:type].to_sym) || (halt 500)

  time = {
    :start => date_start.to_i,
    :end => date_end.to_i,
    :unit => unit,
    :zone => zone
  }

  users = users.split(",").uniq

  return_val = {}

  result = UsageData.fetch_per_user_data(cluster,users,time,type)

  return_val = ""

  return_val << %w|time|.concat(users).join(",") << "\n"

  user_data = {}
  result[:users].each do |data|
    user_data[data[:user]] = data[:data]
  end

  tz = TZInfo::Timezone.get(zone)

  i = 0
  result[:times].each do |time|
    local_time = tz.utc_to_local(Time.at(time/1000).getutc)
    return_val << local_time.strftime("%Y-%m-%d") << ","
    # convert minutes to hours
    return_val << users.map { |user| (user_data[user][i]/60.0).to_s }.join(",") << "\n"
    i += 1
  end

  return_val
end