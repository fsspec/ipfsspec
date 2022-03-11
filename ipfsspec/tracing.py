import asyncio
import aiohttp


class GatewayTracer:
    def __init__(self):
        from collections import defaultdict
        self.samples = defaultdict(list)

    def make_trace_config(self):
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(self.on_request_start)
        trace_config.on_request_end.append(self.on_request_end)
        return trace_config

    async def on_request_start(self, session, trace_config_ctx, params):
        trace_config_ctx.start = asyncio.get_event_loop().time()

    async def on_request_end(self, session, trace_config_ctx, params):
        trace_config_ctx.end = asyncio.get_event_loop().time()
        elapsed = trace_config_ctx.end - trace_config_ctx.start
        status = params.response.status
        gateway = trace_config_ctx.trace_request_ctx.get("gateway", None)
        self.samples[gateway].append({"url": params.url, "method": params.method, "elapsed": elapsed, "status": status})
