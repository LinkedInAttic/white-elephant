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

require 'jruby'

java_import org.apache.hadoop.conf.Configuration
java_import org.apache.hadoop.fs.FileStatus
java_import org.apache.hadoop.fs.FileSystem
java_import org.apache.hadoop.fs.Path
java_import org.apache.hadoop.security.UserGroupInformation

class UsageHadoopLoader < UsageLoader

  def initialize(config)
    super(config)
    @config = config
  end

  def before_load

    conf_dir = @config["usage_loading"]["hadoop"]["conf_dir"]

    # config files must be in classpath for hadoop to find them
    unless $CLASSPATH.include? conf_dir
      $CLASSPATH << conf_dir
    end
    
    # The JRuby class loader should be able to find resources on the classpath above.
    jruby_class_loader = JRuby.runtime.getJRubyClassLoader

    # as a sanity check, ensure the JRuby class loader can find the Hadoop config files

    core_site = jruby_class_loader.getResource("core-site.xml")
    hdfs_site = jruby_class_loader.getResource("hdfs-site.xml")

    raise "Could not find core-site.xml" unless core_site
    puts "Found #{core_site}"

    raise "Could not find hdfs-site.xml" unless hdfs_site
    puts "Found #{hdfs_site}"

    # Make Hadoop use the JRuby class loader so it finds the resources.  It appears that
    # otherwise it will use a different class loader.
    Java::java.lang.Thread.currentThread.setContextClassLoader(jruby_class_loader)

    @conf = Configuration.new(true)

    puts "Loaded configuration, fs.default.name: #{@conf.get('fs.default.name')}"

    @conf.set("hadoop.security.group.mapping", "org.apache.hadoop.security.ShellBasedUnixGroupsMapping")

    UserGroupInformation.setConfiguration(@conf)

    principal = @config["usage_loading"]["hadoop"]["secure"]["principal"]
    keytab_file = @config["usage_loading"]["hadoop"]["secure"]["keytab"]

    puts "Logging in as #{principal} with keytab #{keytab_file}"

    UserGroupInformation.loginUserFromKeytab(principal, keytab_file)

    @fs = FileSystem.get(@conf)

    puts "done"
  rescue Exception => ex
    puts "Failed: " + ex.to_s
    raise ex
  end

  def after_load

  end

  def list_files
    file_pattern = @config["usage_loading"]["hadoop"]["file_pattern"]
    @fs.globStatus(Path.new(file_pattern)).map do |file|
      modified_time = Time.at(file.modification_time/1000)
      [file.get_path.to_s,modified_time]
    end
  end

  def get_local_file(file_name)
    temp_file = Java::java.io.File.createTempFile("we","usage").to_s
    @fs.copyToLocalFile(Path.new(file_name),Path.new(temp_file))
    temp_file
  end
end