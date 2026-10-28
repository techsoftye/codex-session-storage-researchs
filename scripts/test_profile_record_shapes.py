"""Synthetic privacy and shape tests; no copied sessions are read."""
import unittest

import profile_record_shapes as profiler


class ProfileTests(unittest.TestCase):
    def test_dynamic_keys_and_long_values_are_not_exported(self):
        record = {"type": "response_item", "payload": {
            "type": "message", "content": [{"type": "input_text", "text": "private message"}],
            "changes": {r"C:\private\secret.txt": {"content": "private file body"}}}}
        paths, truncated = profiler.key_shape(record)
        self.assertFalse(truncated)
        self.assertIn("payload.changes.{dynamic_key}.content", paths)
        self.assertNotIn("private", " ".join(paths))
        self.assertNotIn("secret.txt", " ".join(paths))
        values = profiler.selected_values(record)
        self.assertEqual(values, [("type", "response_item"), ("payload.type", "message"),
                                  ("payload.content[].type", "input_text")])

    def test_only_short_enum_values_are_retained(self):
        self.assertIsNone(profiler.enum_value(r"C:\private\secret.txt"))
        self.assertIsNone(profiler.enum_value("a" * 81))
        self.assertEqual(profiler.enum_value("function_call_output"), "function_call_output")


if __name__ == "__main__":
    unittest.main()
