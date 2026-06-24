"""Regression tests for streaming proxy generator completion."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from llm_rosetta._vendor.httpclient import StreamingResponse as HttpStreamingResponse
from llm_rosetta.gateway.proxy import _stream_event_generator


class TestStreamEventGenerator:
    """Ensure the upstream SSE loop terminates after OpenAI [DONE]."""

    def test_completes_after_openai_done_without_second_read(self):
        """Must not call aiter_lines() twice — the second read can hang forever."""

        async def _run_test() -> tuple[int, list[str]]:
            sse_lines = [
                'data: {"id":"1","choices":[{"delta":{"content":"hi"}}]}',
                "data: [DONE]",
            ]
            aiter_calls = 0

            async def mock_aiter_lines():
                nonlocal aiter_calls
                aiter_calls += 1
                if aiter_calls > 1:
                    await asyncio.sleep(3600)
                for line in sse_lines:
                    yield line

            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.headers = {}
            mock_resp.aiter_lines = mock_aiter_lines
            mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
            mock_resp.__aexit__ = AsyncMock(return_value=None)

            real_isinstance = isinstance

            def _isinstance(obj: object, cls: type) -> bool:
                if cls is HttpStreamingResponse and hasattr(obj, "aiter_lines"):
                    return True
                return real_isinstance(obj, cls)

            mock_client = MagicMock()
            mock_client.post = AsyncMock(return_value=mock_resp)

            target_converter = MagicMock()
            target_converter.create_stream_context.return_value = MagicMock(
                metadata={}, options={}
            )
            target_converter.stream_response_from_provider.return_value = [
                {"type": "content", "text": "hi"}
            ]

            source_converter = MagicMock()
            source_converter.create_stream_context.return_value = MagicMock(
                metadata={}, options={}
            )
            source_converter.stream_response_to_provider.return_value = {
                "choices": [{"delta": {"content": "hi"}}]
            }

            ctx = MagicMock(metadata={})

            with patch(
                "llm_rosetta.gateway.proxy.get_client", return_value=mock_client
            ):
                with patch("llm_rosetta.gateway.proxy.finalize_stream_request_log"):
                    with patch("builtins.isinstance", _isinstance):
                        gen = _stream_event_generator(
                            source_provider="openai_chat",
                            target_provider="openai_chat",
                            source_converter=source_converter,
                            target_converter=target_converter,
                            ctx=ctx,
                            provider_info=MagicMock(proxy_url=None),
                            url="http://upstream/v1/chat/completions",
                            upstream_body={"model": "gpt-test", "messages": []},
                            headers={},
                            format_sse=lambda chunk: f"data: {chunk}\n\n",
                            store=MagicMock(),
                            model="gpt-test",
                        )

                        chunks: list[str] = []
                        async for chunk in gen:
                            chunks.append(chunk)

            return aiter_calls, chunks

        aiter_calls, chunks = asyncio.run(_run_test())
        assert aiter_calls == 1
        assert any("[DONE]" in chunk for chunk in chunks)
