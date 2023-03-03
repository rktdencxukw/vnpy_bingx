# vnpy_bingx
bingx永续v2接口
由于bingx websocket数据使用gzip压缩了websocket_client里面要改下

    async def _run(self):
        """
        在事件循环中运行的主协程
        """
        # 限制超时300秒，连接池数量300
        timeout = ClientTimeout(total = 300)
        connector = TCPConnector(limit=300, verify_ssl=False)
        self._session: ClientSession = ClientSession(trust_env=True,timeout = timeout,connector = connector)
        while self._active:
            # 捕捉运行过程中异常
            try:
                # 发起Websocket连接
                self._ws = await self._session.ws_connect(
                    self._host,
                    proxy=self._proxy,
                    heartbeat= self._ping_interval,
                    verify_ssl = False,
                )
                # 调用连接成功回调
                self.on_connected()

                # 持续处理收到的数据
                async for msg in self._ws:
                    text: str = msg.data
                    # 解压gzip数据
                    if isinstance(text,bytes):
                        text = gzip.decompress(text)
                        text = text.decode('utf-8')
                        if text == "Ping":
                            self.on_packet(text)
                            continue
                    self._record_last_received_text(text)
                    try:
                        data: dict = self.unpack_data(text)
                        self.on_packet(data)
