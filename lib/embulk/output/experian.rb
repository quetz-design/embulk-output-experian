require "pathname"
require "fileutils"
require "tmpdir"
require "httpclient"

module Embulk
  module Output
    class Experian < OutputPlugin
      Plugin.register_output("experian", self)

      attr_reader :tmpfile

      def self.transaction(config, schema, count, &control)
        @header = schema.collect { |s| s[:name] }

        # configuration code:
        task = {
          tmpdir: config.param("tmpdir", :string, default: Dir.tmpdir),
          tmpfile_prefix: config.param("tmpfile_prefix", :string, default: Time.now.strftime('%Y%m%d_%H%M%S_%N_')),
          cleanup_tmpfiles: config.param("cleanup_tmpfiles", :bool, default: true),

          site_id: config.param("site_id", :string),
          login_id: config.param("login_id", :string),
          password: config.param("password", :string),
          encoding: config.param("encoding", :string, default: "shift_jis"),
          csvfile_id: config.param("csvfile_id", :integer),
          draft_id: config.param("draft_id", :integer),
          book_time: config.param("book_time", :string),
          unique_name: config.param("unique_name", :string, default: "reserved by plugin.")
        }

        # resumable output:
        # resume(task, schema, count, &control)

        # non-resumable output:
        FileUtils.mkdir_p task[:tmpdir]
        task_reports = yield(task)

        upload(task)
        # check(task)
        # reserve(task)

        next_config_diff = {}
        return next_config_diff
      end

      def init
        # initialization code:
        @tmpfile = Pathname.new(task[:tmpdir]).join("#{task[:tmpfile_prefix]}_#{index}.csv")
        Embulk.logger.debug "Partial CSV will be stored as #{@tmpfile}"
      end

      def close
      end

      def add(page)
        csv = ""
        page.each do |record|
          hash = Hash[schema.names.zip(record)]
          csv_line = format_csv(hash)
          csv << csv_line << "\n"
        end
        @csv = csv
      end

      def finish
        generate_csv_file
      end

      def abort
      end

      def commit
        task_report = {}
        return task_report
      end

      private

      def format_csv(record_hash)
        record_hash.values.map do |rec|
          rec
        end.join(",")
      end

      def generate_csv_file
        File.open(@tmpfile, "a") do |f|
          f.write @csv
        end
        @csv = ""
      end

      def self.upload(task)
        csv_path = union_single_csv_file(task)
        Embulk.logger.debug "Whole CSV file path: #{csv_path}"
        Client.new(task).upload_csv(csv_path)
      ensure
        if task[:cleanup_tmpfiles]
          tmp_path = Pathname.new(task[:tmpdir]).join(task[:tmpfile_prefix])
          Dir.glob("#{tmp_path}*.csv") do |file|
            Embulk.logger.debug "Deleting #{file} (due to `cleanup_tmpfiles` is true)"
            File.unlink file
          end
        end
      end

      def self.union_single_csv_file(task)
        prefix = Pathname.new(task[:tmpdir]).join(task[:tmpfile_prefix])
        all_csv_path = "#{prefix}_all.csv"

        File.open(all_csv_path, "w", :encoding=>task[:encoding]) do |all_csv|
          all_csv.puts @header.join(",") # CSV header is required
          Dir.glob("#{prefix}*.csv") do |partial_csv|
            partial = File.read(partial_csv)

            if task[:encoding] == "shift_jis"
              partial.encode!(:encoding=>"shift_jis:utf-8", :invalid=>:replace, :undef=>:replace)
            end

            all_csv.write partial
          end
        end
        all_csv_path
      end
    end

    class Client
      attr_reader :task

      def initialize(task)
        @task = task
      end

      def upload_csv(csv_path)
        begin
          Embulk.logger.info "csv: #{csv_path}"
          File.open(csv_path) do |csv|
            params = {
              login_id: task[:login_id],
              password: task[:password],
              FILE: csv,
              id: task[:csvfile_id],
              title: task[:unique_name],
              post_use_utf8: true,
            }
            upload_url = "https://remote2.rec.mpse.jp/#{task[:site_id]}/remote/upload.php"
            if task[:encoding] == "utf-8"
              params[:list_use_utf8] = "utf-8"
            end

            Embulk.logger.info "params: #{task[:csvfile_id]}, #{task[:unique_name]}, #{params.id}, #{task[:title]}"

            response = httpclient.post(upload_url, params)
            handle_error(response)
          end
        rescue TooFrequencyError
          # NOTE: Quoted from document Sec 3.1:
          # > Request frequency
          # > The system will not receive the same type of request within 15 seconds (an error will be returned). Please allow a 15 second wait time before requests.
          Embulk.logger.warn "Got 400 error (frequency access). retry after 15 seconds"
          sleep 15
          retry
        end
      end

      private

      def httpclient
        httpclient = HTTPClient.new
        # httpclient.debug_dev = STDOUT # for debugging
        httpclient
      end

      def handle_error(response)
        body = response.body
        case response.status
        when 200
          # ok
        when 400
          if body.include?("ERROR=アクセス間隔が短すぎます。時間を置いて再度実行してください")
            raise TooFrequencyError
          else
            raise "[#{response.status}] #{body}"
          end
        else
          raise "[#{response.status}] #{body}"
        end
      end
    end

    class TooFrequencyError < StandardError; end
  end
end
