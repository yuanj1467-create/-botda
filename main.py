--- a/main.py
+++ b/main.py
@@
     async def worker(self, channel: discord.TextChannel):
@@
         while getattr(self, 'running', False) and (self.forever or self.checked < MAX_ATTEMPTS):
@@
                 print(f"[FOUND-ENQUEUED] {result['code']} -> {result['guild']}")
@@
             if self.checked % 50 == 0:
                 print(f"進捗: {self.checked}件チェック済み / 発見: {len(self.found)}件")
+
+    async def _maintain_workers(self, channel: discord.TextChannel, desired_count: int):
+        """
+        Maintain approximately `desired_count` worker tasks while scanner is running.
+        If a worker exits (exception or normal), spawn a replacement after a short jitter.
+        """
+        tasks = {asyncio.create_task(self.worker(channel)) for _ in range(desired_count)}
+        try:
+            while getattr(self, 'running', False):
+                if not tasks:
+                    # if tasks set became empty, create at least one worker
+                    tasks = {asyncio.create_task(self.worker(channel)) for _ in range(desired_count)}
+                done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
+                for t in done:
+                    tasks.discard(t)
+                    try:
+                        exc = t.exception()
+                        if exc:
+                            print(f"worker task exited with exception: {exc}")
+                        else:
+                            print("worker task finished normally")
+                    except asyncio.CancelledError:
+                        print("worker task cancelled")
+                    # if still running, replace the worker after a small jitter
+                    if getattr(self, 'running', False):
+                        await asyncio.sleep(random.uniform(0.5, 1.5))
+                        tasks.add(asyncio.create_task(self.worker(channel)))
+            # shutdown remaining tasks
+            for t in tasks:
+                t.cancel()
+            await asyncio.gather(*tasks, return_exceptions=True)
+        except Exception as e:
+            print(f"_maintain_workers exception: {e}")
+            for t in tasks:
+                try:
+                    t.cancel()
+                except Exception:
+                    pass
+            await asyncio.gather(*tasks, return_exceptions=True)
@@
-    await ctx.send(f"🔁 永続スキャン開始（停止コマンドで停止）\n対象チャンネル: {target.mention}")
-
-    workers = [asyncio.create_task(scanner.worker(target)) for _ in range(MAX_WORKERS)]
-    await asyncio.gather(*workers, return_exceptions=True)
+    await ctx.send(f"🔁 永続スキャン開始（停止コマンドで停止）\n対象チャンネル: {target.mention}")
+
+    # Maintain workers concurrently, replacing any that die to keep throughput steadier
+    await scanner._maintain_workers(target, MAX_WORKERS)
*** End Patch