import io
import json
import unittest

from profile_response_items import add_record, merge, new_stats, scan_session


class StructureTests(unittest.TestCase):
    def test_counts_multiple_content_blocks_once_per_record_without_exporting_values(self):
        stats = new_stats()
        value = {"type":"response_item", "payload":{"type":"message", "role":"user", "content":[
            {"type":"input_text", "text":"private instruction"},
            {"type":"input_text", "text":"another private instruction"},
            {"type":"input_image", "image_url":"data:image/png;base64," + "A"*100000},
        ]}}
        add_record(stats,value,123)
        group = stats["subtypes"]["message"]
        self.assertEqual(group["fields"]["payload.content[].type : string"], {"records":1,"occurrences":3})
        self.assertEqual(group["enums"]["payload.content[].type"], {"input_text":2,"input_image":1})
        self.assertEqual(group["features"]["embedded_base64_image"],1)
        self.assertNotIn("private instruction",json.dumps(stats))
        self.assertNotIn("AAAAA",json.dumps(stats))

    def test_exact_ranges_ignore_surrounding_records_and_support_reordered_keys(self):
        prefix = b'not in the authorized range\n'
        item = json.dumps({"payload":{"role":"assistant","type":"message","content":[]},
                           "type":"response_item"}).encode() + b'\n'
        stats = new_stats()
        scan_session(io.BytesIO(prefix+item+b'invalid suffix'),[(1,2,len(prefix),len(item))],stats)
        self.assertEqual(stats["records"],1)
        self.assertEqual(stats["source_bytes"],len(item))
        self.assertEqual(stats["failed_records"],0)

    def test_invalid_or_duplicate_json_is_counted_as_failure_without_exporting_error_text(self):
        for raw in (b'{"type":"response_item","type":"secret"}',b'not-json private text',
                    b'{"type":"event_msg","payload":{"type":"response_item"}}'):
            stats = new_stats()
            scan_session(io.BytesIO(raw),[(1,9,0,len(raw))],stats)
            self.assertEqual(stats["records"],0)
            self.assertEqual(stats["failed_records"],1)
            self.assertEqual(stats["failures"][0]["record_id"],9)
            self.assertNotIn("private",json.dumps(stats))

    def test_checkpoint_aggregation_retains_record_and_occurrence_counts(self):
        a,b = new_stats(),new_stats()
        record = {"type":"response_item","payload":{"type":"function_call_output","output":"text"}}
        add_record(a,record,10)
        add_record(b,record,20)
        merge(a,b)
        self.assertEqual(a["records"],2)
        self.assertEqual(a["source_bytes"],30)
        self.assertEqual(a["subtypes"]["function_call_output"]["fields"]["payload.output : string"]["records"],2)

    def test_event_profile_exports_counts_without_conversation_or_link_values(self):
        stats = new_stats()
        value = {"type":"event_msg", "payload":{"type":"exec_command_end",
                 "turn_id":"private-turn-value", "message":"private conversation",
                 "status":"completed", "changes":{"private_filename":{"type":"update"}}}}
        add_record(stats,value,100,"event_msg")
        group = stats["subtypes"]["exec_command_end"]
        self.assertEqual(group["enums"]["payload.status"],{"completed":1})
        self.assertEqual(group["link_field_coverage"]["payload.turn_id"]["records_present"],1)
        for private in ("private-turn-value","private conversation","private_filename"):
            self.assertNotIn(private,json.dumps(stats))

    def test_event_link_coverage_distinguishes_occurrences_and_records(self):
        a,b = new_stats(),new_stats()
        value = {"type":"event_msg", "payload":{"type":"example", "items":[
            {"call_id":"a"},{"call_id":"b"},{"call_id":None}]}}
        add_record(a,value,90,"event_msg")
        coverage = a["subtypes"]["example"]["link_field_coverage"]["payload.items[].call_id"]
        self.assertEqual(coverage["records_present"],1)
        self.assertEqual(coverage["occurrences"],3)
        self.assertEqual(coverage["records_with_nonempty_string"],1)
        self.assertEqual(coverage["records_with_null"],1)
        add_record(b,value,90,"event_msg")
        merge(a,b)
        self.assertEqual(coverage["records_present"],2)
        self.assertEqual(coverage["occurrences"],6)

    def test_event_ranges_accept_event_only(self):
        raw = b'{"type":"event_msg","payload":{"type":"user_message","message":"private"}}'
        stats = new_stats()
        scan_session(io.BytesIO(raw),[(1,2,0,len(raw))],stats,"event_msg")
        self.assertEqual(stats["records"],1)
        wrong = new_stats()
        scan_session(io.BytesIO(raw),[(1,2,0,len(raw))],wrong)
        self.assertEqual(wrong["failed_records"],1)


if __name__ == "__main__":
    unittest.main()
