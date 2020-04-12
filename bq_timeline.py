"""
BQ用のtimelineを作成する

Timeline フォーマット情報
https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/edit

クエリプランの情報
https://cloud.google.com/bigquery/query-plan-explanation
"""

import json
import uuid

"""
BQログの1ステップ
        {
          "completedParallelInputs": "2200", 
          "computeMsAvg": "698", 
          "computeMsMax": "1289", 
          "computeRatioAvg": 0.026543961058716155, 
          "computeRatioMax": 0.04901886218436264, 
          "endMs": "1586609427936", 
          "id": "8", 
          "inputStages": [
            "7", 
            "0"
          ], 
          "name": "S08: Aggregate", 
          "parallelInputs": "2200", 
          "readMsAvg": "0", 
          "readMsMax": "0", 
          "readRatioAvg": 0, 
          "readRatioMax": 0, 
          "recordsRead": "1205625503", 
          "recordsWritten": "280512407", 
          "shuffleOutputBytes": "14193408673", 
          "shuffleOutputBytesSpilled": "0", 
          "slotMs": "2455672", 
          "startMs": "1586609426711", 
          "status": "COMPLETE", 
          "steps": [
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $40 := $30",
                "$10 := SUM_OF_COUNTS($20)"
              ]
            }
          ], 
          "waitMsAvg": "278", 
          "waitMsMax": "338", 
          "waitRatioAvg": 0.010571950106480073, 
          "waitRatioMax": 0.012853665956799513, 
          "writeMsAvg": "83", 
          "writeMsMax": "641", 
          "writeRatioAvg": 0.0031563735929418923, 
          "writeRatioMax": 0.024376331000912688
        }, 

"""


class Stage(object):
    """BQのステージ (S00とか) を表すクラス

    """

    def __init__(self, json_dict):
        """このStageに該当するJsonデータからオブジェクトを生成する

        :param dict json_dict: Stage情報を格納したdictionary
        :return:
        """
        self._original_dict = json_dict
        d = json_dict

        self.stage_id = int(d["id"])  # "8"
        self.name = str(d["name"])  # "S08: Aggregate",
        self.status = d["status"]  # "COMPLETE" 基本的にcompleteを対象にしているので利用しない
        assert self.status == "COMPLETE"

        # 一番最初のステージはinputStagesを持たない
        if "inputStages" in d:
            self.input_stages = [int(tmp) for tmp in d["inputStages"]]  # ステージの依存関係グラフを形成する ID のリスト
        else:
            self.input_stages = None

        self.start_ms = int(d["startMs"])  # "1586609426711",
        self.end_ms = int(d["endMs"])  # "1586609427936",
        self.duration_ms = self.end_ms - self.start_ms

        self.record_reads = int(d["recordsRead"])  # ステージ ワーカー全体でのステージの入力サイズ (レコード数) "1205625503",
        self.record_written = int(d["recordsWritten"])  # ステージ ワーカー全体でのステージの出力サイズ（レコード数）。"280512407"

        # ステージで同時に読み込み可能な作業単位の数。ステージとクエリに応じて、テーブルの列セグメントの数や、
        # 中間シャッフル内のパーティションの数を表す場合があります。 "2200"
        self.parallel_inputs = d["parallelInputs"]
        self.completed_parallel_inputs = d["completedParallelInputs"]  # 上と同値前提なので使わない
        assert self.parallel_inputs == self.completed_parallel_inputs

        self.shuffle_output_bytes = int(d["shuffleOutputBytes"])  # ワーカーの書き込みバイト数 "14193408673",
        self.shuffle_output_bytes_spilled = int(d["shuffleOutputBytesSpilled"])  # ディスクにオーバーフローしたバイト数 "0",
        # 多くの場合 0
        assert self.shuffle_output_bytes_spilled == 0

        str_steps = ""

        # stepsの解析はパターンが多いので後々やる
        for cur_step in d["steps"]:
            """
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $40 := $30",
                "$10 := SUM_OF_COUNTS($20)"
              ]
            """
            kind = cur_step["kind"]  # READ, WRITE, COMPUTE, etc...
            str_steps += f"[{kind}]\n"
            substeps = cur_step["substeps"]  # 実行されるsubstep
            for s in substeps:
                str_steps += s + "\n"

        self.steps = str_steps

        # おそらくトータルで利用したスロット時間
        self.slot_ms = int(d["slotMs"])  # "2455672",

        """
        wait, read, compute, writeに費やした時間の平均と最大情報
        比率形式と絶対値形式がある。
        比率形式の場合、*おそらく*すべてのワーカーの中で最長の時間のものとの比率であらわされる。
        すべてのワーカーとはこのクエリに参加したすべてのワーカーを指す。
        wait, read, compute, write, すべてのワーカーの中で最長のものを基準としているため、
        readRatioMax, computeRatioMax, writeRatioMaxの中で1になるのは一つだけ (の筈) 
        """
        # MEMO (oiki) : スケジュール完了の待機とは？

        self.wait_ms_avg = int(d["waitMsAvg"])  # "278",
        self.wait_ms_max = int(d["waitMsMax"])  # "338",
        self.wait_ratio_avg = float(d["waitRatioAvg"])  # 0.010571950106480073,
        self.wait_ratio_max = float(d["waitRatioMax"])  # 0.012853665956799513,
        self.read_ms_avg = int(d["readMsAvg"])  # "0",
        self.read_ms_max = int(d["readMsMax"])  # "0",
        self.read_ratio_avg = float(d["readRatioAvg"])  # 0,
        self.read_ratio_max = float(d["readRatioMax"])  # 0,
        self.compute_ms_avg = int(d["computeMsAvg"])  # "698",
        self.compute_ms_max = int(d["computeMsMax"])  # "1289",
        self.compute_ratio_avg = float(d["computeRatioAvg"])  # 0.026543961058716155,
        self.compute_ratio_max = float(d["computeRatioMax"])  # 0.04901886218436264,
        self.write_ms_avg = int(d["writeMsAvg"])  # "83",
        self.write_ms_max = int(d["writeMsMax"])  # "641",
        self.write_ratio_avg = float(d["writeRatioAvg"])  # 0.0031563735929418923,
        self.write_ratio_max = float(d["writeRatioMax"])  # 0.024376331000912688

        self.wait_substage, self.read_substage, self.compute_substage, self.write_substage = self._make_substages(
            mode="avg")
        self.substages = [tmp for tmp in
                          [self.wait_substage, self.read_substage, self.compute_substage, self.write_substage] if
                          tmp is not None]

    def _make_substages(self, mode="avg"):
        """wait, read, compute, writeのsubstagesを出力する

        :param str mode: どちらの時間を基準にして返すか (avg or max)
        :return:
        """
        """
        各substageの正確な実行タイミングはわからない。
        
        そこで、wait, read, computeはstageの開始と同時にはじまり, writeはstageの終了と同時に終了するものとする
        """
        start_ms = self.start_ms
        end_ms = self.end_ms

        if self.wait_ms_avg == 0:
            wait_substage = None
        else:
            if mode == "avg":
                cur_dur = self.wait_ms_avg
            else:
                cur_dur = self.wait_ms_max
            wait_substage = SubStage(substage_id=str(uuid.uuid4()),
                                     kind="wait", start_ms=start_ms, end_ms=start_ms + cur_dur,
                                     avg_ms=self.wait_ms_avg, max_ms=self.wait_ms_max)

        if mode == "avg":
            cur_dur = self.read_ms_avg
        else:
            cur_dur = self.read_ms_max

        read_desc = f"Record Read {self.record_reads} records\n"
        read_desc += f"Parallel Inputs {self.parallel_inputs}\n"
        read_substage = SubStage(substage_id=str(uuid.uuid4()),
                                 kind="read", start_ms=start_ms, end_ms=start_ms + cur_dur,
                                 avg_ms=self.read_ms_avg, max_ms=self.read_ms_max,
                                 description=read_desc)

        if mode == "avg":
            cur_dur = self.compute_ms_avg
        else:
            cur_dur = self.compute_ms_max
        compute_desc = self.steps
        compute_substage = SubStage(substage_id=str(uuid.uuid4()),
                                    kind="compute", start_ms=start_ms, end_ms=start_ms + cur_dur,
                                    avg_ms=self.compute_ms_avg, max_ms=self.compute_ms_max,
                                    description=compute_desc)

        if mode == "avg":
            cur_dur = self.write_ms_avg
        else:
            cur_dur = self.write_ms_max
        write_desc = f"Records Written {self.record_written} records"
        write_desc += f"Shuffle Output Bytes {self.shuffle_output_bytes} bytes Spilled {self.shuffle_output_bytes_spilled} bytes"
        write_substage = SubStage(substage_id=str(uuid.uuid4()),
                                  kind="write", start_ms=end_ms - cur_dur, end_ms=end_ms,
                                  avg_ms=self.write_ms_avg, max_ms=self.write_ms_max,
                                  description=write_desc)

        return wait_substage, read_substage, compute_substage, write_substage


class SubStage(object):
    """Stageの各ステップを表すオブジェクト

    wait, read, compute, writeのどれかを表すときと
    substepsの各ステップを表すときがある

    """

    def __init__(self, substage_id, kind, start_ms, end_ms, avg_ms, max_ms, description=""):
        """

        :param str substage_id: substage内で一意のID
        :param kind:
        :param start_ms:
        :param end_ms:
        :param description:
        """
        self.substage_id = substage_id
        self.kind = kind
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.avg_ms = avg_ms
        self.max_ms = max_ms
        self._description = description

    @property
    def name(self):
        return self.kind

    @property
    def description(self):
        desc = f"Avg {self.avg_ms} ms Max {self.avg_ms} ms\n"

        return desc + self._description


class SamplingTimeline(object):
    """サンプリングで集められたユニット情報 (サンプリング周期は？)

    "timeline": [
    {
      "activeUnits": "70",
      "completedUnits": "0",
      "elapsedMs": "600",
      "pendingUnits": "220",
      "totalSlotMs": "8828"
    }
    """

    class SamplingEvent(object):
        def __init__(self, start_ms, active_units, completed_units):
            self.start_ms = start_ms
            self.active_units = active_units
            self.completed_units = completed_units

    def __init__(self, start_ms, json_timeline):
        self._json_timeline = json_timeline
        events = []

        for event in self._json_timeline:
            active_units = int(event["activeUnits"])
            completed_units = int(event["completedUnits"])
            ts = int(event["elapsedMs"])
            pending_units = int(event["pendingUnits"])
            total_slot_ms = int(event["totalSlotMs"])

            cur_event = self.SamplingEvent(
                active_units=active_units,
                completed_units=completed_units,
                start_ms=start_ms + ts
            )

            events.append(cur_event)

        self.events = events


class Query(object):
    """クエリプランに該当するクラス

    ジョブ情報のstatistics.query 以下の情報を扱う

    """

    def __init__(self, start_ms, json_dict):
        self.estimated_bytes_processed = int(json_dict["estimatedBytesProcessed"])  # "39931898771"
        self.total_bytes_billed = int(json_dict["totalBytesBilled"])  # "39932919808",
        self.total_bytes_processed = int(json_dict["totalBytesProcessed"])  # "39931898771",
        self.tatal_partitions_processed = int(json_dict["totalPartitionsProcessed"])  # "0",
        self.total_slot_ms = int(json_dict["totalSlotMs"])  # "9538178"
        # 参照先テーブル (未使用)
        self.referenced_tables = json_dict["referencedTables"]

        query_plan = json_dict["queryPlan"]

        stages = []

        for cur_stage in query_plan:
            stages.append(Stage(cur_stage))

        self.stages = stages
        self.stage_id2stage = {}

        for cur_stage in stages:
            self.stage_id2stage[cur_stage.stage_id] = cur_stage

        self.sampling_timeline = SamplingTimeline(start_ms, json_dict["timeline"])


class JobInfo(object):
    """ジョブの実行情報を表すオブジェクト
    """

    def __init__(self, json_dict):
        self.query_str = self._read_configuration(json_dict["configuration"])
        self.job_id = json_dict["id"]  # "sample-ml:US.bquxjob_7dcf868c_17169496fde",
        stat = json_dict["statistics"]

        # msであらわされた開始時間
        self.creation_ms = int(stat["creationTime"])
        self.start_ms = int(stat["startTime"])
        self.endt_ms = int(stat["endTime"])

        self.total_bytes_processed = int(stat["totalBytesProcessed"])  # "39931898771",
        self.total_slot_ms = int(stat["totalSlotMs"])  # "9538178"

        self.query_plan = Query(start_ms=self.creation_ms, json_dict=stat["query"])

    @property
    def stages(self):
        return self.query_plan.stages

    def _read_configuration(self, json_conf):
        """ジョブのコンフィグ情報を取得する

        実行したqueryの文字列を返す
        """
        query = json_conf["query"]
        return query


class BQTimeline(object):
    """BQのジョブログからTrace Eventを形成する

    pid : それぞれのステージに割り当てる
    tid : ステージ内のwait, read, compute, writeに割り当てる (将来的にはsubstepsに割り当てたい)

    各substageのイベントは以下の列であらわす。


    """
    COUNTER_PID = 2000

    def __init__(self, job_info):
        """

        :param JobInfo job_info:
        """
        self._job_info = job_info
        self._stage2pid, self._substage2tid, self._pid2name, self._tid2name = self._assign_pid_tid(self._job_info)

    @classmethod
    def _assign_pid_tid(cls, job_info):
        """ジョブのStageにpidを, waitやreadなどにtidを割り当てる

        :param JobInfo job_info:
        :rtype: dict
        :return:
          stage2pid : key=>stage_id, value=>pid
          substage2tid:  key=>substage_id, value=>tid
          pid2name: key=>pid, value=name
          tid2name: key=>tid, value=name
        """
        cur_pid = 100
        cur_tid = 1000
        stage2pid = {}
        substage2tid = {}
        pid2name = {}
        tid2name = {}

        for cur_stage in job_info.stages:
            cur_stage_id = cur_stage.stage_id
            stage2pid[cur_stage_id] = cur_pid
            pid2name[cur_pid] = cur_stage.name
            cur_pid += 1

            for cur_sub_stage in cur_stage.substages:
                # 数値を割り当てていたが、表示名としてもつかわれるのでカテゴリにした
                # substage2tid[cur_sub_stage.substage_id] = cur_tid
                substage2tid[cur_sub_stage.substage_id] = cur_sub_stage.kind
                tid2name[cur_tid] = cur_sub_stage.name
                cur_tid += 1

        return stage2pid, substage2tid, pid2name, tid2name

    def _generate_trace_dict(self):
        dict_trace = {}
        trace_events = []

        trace_events += self._generate_metadata_events()
        trace_events += self._generate_substage_events()
        trace_events += self._generate_data_flow()
        trace_events += self._generate_units_count()

        dict_trace["traceEvents"] = trace_events

        # メタデータ群
        dict_trace["displayTimeUnit"] = "ms"
        dict_trace["otherData"] = {
            "otherData": {
                "version": "BQ Trace Viewer v0.0.1"
            }
        }

        return dict_trace

    def _generate_substage_events(self):
        """各substageのTimeline情報を出力する

        :rtype: list[dict]
        :return:
        """
        timelines = []

        for cur_stage in self._job_info.stages:
            for cur_substage in cur_stage.substages:
                d = {
                    "name": cur_substage.name,
                    "cat": cur_substage.kind,
                    "ph": "X",
                    "pid": self._stage2pid[cur_stage.stage_id],
                    "tid": self._substage2tid[cur_substage.substage_id],
                    "ts": cur_substage.start_ms * 1000,
                    "dur": (cur_substage.end_ms - cur_substage.start_ms) * 1000,
                    "args": {"desc": cur_substage.description}
                }

                timelines.append(d)

        return timelines

    def _generate_metadata_events(self):
        """プロセス名、スレッド名に関する情報を生成する

        :return:
        """
        name_mapping = []

        for pid, name in self._pid2name.items():
            d = {
                "name": "process_name",
                "ph": "M",
                "pid": pid,
                "args": {"name": name}
            }
            name_mapping.append(d)

        # counter用の特別なPID
        d = {
            "name": "process_name",
            "ph": "M",
            "pid": self.COUNTER_PID,
            "args": {"name": "Unit Counter"}
        }
        name_mapping.append(d)

        for tid, name in self._tid2name.items():
            d = {
                "name": "thrad_name",
                "ph": "M",
                "tid": tid,
                "args": {"name": name}
            }
            name_mapping.append(d)

        return name_mapping

    def _generate_data_flow(self):
        """データの受け渡し関係のFlowを追加する

        :return:
        """
        flow_events = []
        next_flow_id = 0

        for cur_dest_stage in self._job_info.stages:
            if cur_dest_stage.input_stages is None:
                continue

            for input_stage_id in cur_dest_stage.input_stages:
                # sourceからdestへのflowをはる
                cur_source_stage = self._job_info.query_plan.stage_id2stage[input_stage_id]
                # MEMO (oiki) : 本来はwriteとreadの間に依存関係があるが、時間的にstart <= end である必要があるため
                # cur_source_substage = cur_source_stage.write_substage
                # cur_dest_substage = cur_dest_stage.read_substage
                cur_source_substage = cur_source_stage.read_substage
                cur_dest_substage = cur_dest_stage.read_substage

                source_pid = self._stage2pid[input_stage_id]
                source_tid = self._substage2tid[cur_source_substage.substage_id]

                dest_pid = self._stage2pid[cur_dest_stage.stage_id]
                dest_tid = self._substage2tid[cur_dest_substage.substage_id]

                flow_start_ms = cur_source_substage.start_ms
                flow_end_ms = cur_dest_substage.start_ms

                d_source = {
                    "name": "sink",  # TODO (oiki) : sink名にする
                    "cat": "DataFlow",
                    "ph": "s",
                    "id": next_flow_id,
                    "pid": source_pid,
                    "tid": source_tid,
                    "ts": flow_start_ms * 1000
                }
                d_dest = {
                    "name": "sink",  # TODO (oiki) : sink名にする
                    "cat": "DataFlow",
                    "ph": "t",
                    "id": next_flow_id,
                    "pid": dest_pid,
                    "tid": dest_tid,
                    "ts": flow_end_ms * 1000
                }
                next_flow_id += 1

                flow_events.append(d_source)
                flow_events.append(d_dest)

        return flow_events

    def _generate_units_count(self):
        """unit数の時間推移を表すイベントを出力する

        :return:
        """
        events = []
        for cur_event in self._job_info.query_plan.sampling_timeline.events:
            d = {
                "name": "ctr",
                "ph": "C",
                "pid": self.COUNTER_PID,
                "ts": cur_event.start_ms * 1000,
                "args": {
                    "activeUnits": cur_event.active_units,
                    "completedUnits": cur_event.completed_units
                }
            }
            events.append(d)
        return events

    def generate_trace_file(self, path_json):
        dict_trace = self._generate_trace_dict()
        with open(path_json, "w") as f:
            json.dump(dict_trace, f)


def main():
    path_json = "sample.json"
    path_out = "timeline.json"
    with open(path_json, "r") as f:
        dict_json = json.load(f)

    job_info = JobInfo(dict_json)
    timeline = BQTimeline(job_info)
    timeline.generate_trace_file(path_out)


if __name__ == "__main__":
    main()
