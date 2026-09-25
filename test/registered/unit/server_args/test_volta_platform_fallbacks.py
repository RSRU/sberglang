import unittest
from unittest import mock

from sglang.srt.arg_groups.overrides import (
    _sampling_backend_default,
    _volta_attention_backend_fallback,
)
from sglang.srt.model_executor.cuda_graph_config import Backend, cuda_graph_fully_disabled


class _VoltaView:
    device = "cuda"
    attention_backend = None
    prefill_attention_backend = None
    decode_attention_backend = None
    sampling_backend = None
    dtype = "auto"
    cuda_graph_config = None


class TestVoltaPlatformFallbacks(unittest.TestCase):
    @mock.patch("sglang.srt.arg_groups.overrides.is_sm70_volta", return_value=True)
    @mock.patch("sglang.srt.arg_groups.overrides.resolved_view")
    def test_volta_defaults_to_triton_and_fp16(self, mock_resolved_view, _mock_volta):
        view = _VoltaView()
        mock_resolved_view.return_value = view

        overrides = _volta_attention_backend_fallback(view)

        self.assertEqual(overrides["attention_backend"], "triton")
        self.assertEqual(overrides["dtype"], "float16")

    @mock.patch("sglang.srt.arg_groups.overrides.is_sm70_volta", return_value=True)
    def test_volta_sampling_backend_default(self, _mock_volta):
        view = _VoltaView()

        overrides = _sampling_backend_default(view)

        self.assertEqual(overrides["sampling_backend"], "pytorch")

    @mock.patch("sglang.srt.arg_groups.overrides.is_sm70_volta", return_value=True)
    @mock.patch("sglang.srt.arg_groups.overrides.resolved_view")
    def test_volta_rejects_flashinfer_attention(self, mock_resolved_view, _mock_volta):
        view = _VoltaView()
        view.attention_backend = "flashinfer"
        mock_resolved_view.return_value = view

        overrides = _volta_attention_backend_fallback(view)

        self.assertEqual(overrides["attention_backend"], "triton")

    def test_triton_backend_does_not_disable_cuda_graph_by_default(self):
        from sglang.srt.server_args import ServerArgs

        args = ServerArgs(model_path="meta-llama/Llama-3.1-8B-Instruct")
        args.attention_backend = "triton"
        args._handle_attention_backend_compatibility()

        self.assertFalse(cuda_graph_fully_disabled(args.cuda_graph_config))
        self.assertNotEqual(args.cuda_graph_config.decode.backend, Backend.DISABLED)


if __name__ == "__main__":
    unittest.main()
