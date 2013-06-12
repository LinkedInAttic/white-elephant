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

require 'java'
require 'bundler/setup'
require 'coffee_script'
require 'less'
require 'sinatra'
require 'sinatra/content_for'
require 'sinatra/url_for'
require 'json'
require 'rack/cache'
require 'sprockets'
require 'yaml'

configure :development do
  puts "Running in development mode"
end

configure :production do
  puts "Running in production mode"
end

configure :development do |config|
  require "sinatra/reloader"
  
  # don't buffer in dev mode so we can see all output immediately
  $stdout.sync = true

  config.also_reload File.dirname(__FILE__) + "/app/*.rb"
end

set :views, File.dirname(__FILE__) + "/views"
set :logging, true

helpers do
  include Rack::Utils
  alias_method :h, :escape_html
end

require 'app/usage_database'
require 'app/usage_data'

@@config = YAML.load_file(File.dirname(__FILE__) + '/config.yml')

raise "root element 'usage_loading' not found in config" unless @@config["usage_loading"]

if db_config = @@config["usage_loading"]["db"]
  case db_config["mode"]
  when "memory"
    UsageDatabase.use_in_memory_db
  when "disk"
    db_path = db_config["path"]
    raise "Need db path in order to run in disk mode" unless db_path
    UsageDatabase.use_disk_db(db_path)
  end
else
  UsageDatabase.use_in_memory_db
end

UsageDatabase.initialize_db

data_config = @@config["usage_loading"]["data"]

raise "data configuration not found at usage_loading->data" unless data_config

data_mode = data_config["mode"]

loader = case data_mode
when "local"
  puts "Creating local loader"
  require 'app/usage_local_loader'
  local_config = data_config["local"]
  raise "local config not found at usage_loading->data->local" unless local_config
  UsageLocalLoader.new(local_config) 
when "hadoop"
  puts "Creating hadoop loader"
  hadoop_config = data_config["hadoop"]
  raise "local config not found at usage_loading->data->hadoop" unless hadoop_config
  hadoop_lib_dirs = hadoop_config["libs"]
  raise "hadoop lib dirs not found or empty at usage_loading->data->hadoop->libs" unless hadoop_lib_dirs && hadoop_lib_dirs.size > 0

  hadoop_lib_dirs.each do |lib_path|
    puts "Searching for JARs in #{lib_path}"
    Dir[File.join(lib_path,"*.jar")].each do |lib|
      puts "Adding to classpath: #{lib}"
      $CLASSPATH << lib
    end
  end

  require 'app/usage_hadoop_loader'
  UsageHadoopLoader.new(hadoop_config)
else
  raise "data mode must be either 'local' or 'hadoop'"
end

puts "Starting loader"
loader.start