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



if db_config = @@config["usage_loading"] && @@config["usage_loading"]["db"]
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

configure :development do
  require 'app/usage_local_loader'
  UsageLocalLoader.new(@@config).start
end

configure :production do
  @@config["usage_loading"]["hadoop"]["libs"].each do |lib_path|
    puts "Searching for JARs in #{lib_path}"
    Dir[File.join(lib_path,"*.jar")].each do |lib|
      puts "Adding to classpath: #{lib}"
      $CLASSPATH << lib
    end
  end

  require 'app/usage_hadoop_loader'
  UsageHadoopLoader.new(@@config).start
end
