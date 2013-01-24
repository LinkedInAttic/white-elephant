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

require 'app/usage_loader'

class UsageLocalLoader < UsageLoader

  def initialize(config)
    super(config)
    @config = config
  end

  def list_files
    file_pattern = @config["usage_loading"]["local"]["file_pattern"]

    puts "Listing local files in #{file_pattern}"

    Dir[file_pattern].map do |file_name|
      modified_time = File.new(file_name).mtime
      [file_name,modified_time]
    end
  end

  def get_local_file(file_name)
    # file is already local
    file_name
  end
end