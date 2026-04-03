from simple_task_management_system import SimpleTaskManagementSystem


class SimpleTaskManagementSystemImpl(SimpleTaskManagementSystem):
    def __init__(self):
        self.tasks = {}        # task_id -> {name, priority, order}
        self.counter = 0
        self.users = {}        # user_id -> {quota}
        self.assignments = {}  # task_id -> {user_id, deadline, finish_time}
        self.completed = set()

    # ─── Level 1 ───────────────────────────────────────────────────────────────

    def add_task(self, timestamp: int, name: str, priority: int) -> str:
        self.counter += 1
        task_id = f"task_id_{self.counter}"
        self.tasks[task_id] = {
            "name": name,
            "priority": priority,
            "order": self.counter
        }
        return task_id

    def update_task(self, timestamp: int, task_id: str, name: str, priority: int) -> bool:
        if task_id not in self.tasks:
            return False
        self.tasks[task_id]["name"] = name
        self.tasks[task_id]["priority"] = priority
        return True

    def get_task(self, timestamp: int, task_id: str) -> str | None:
        if task_id not in self.tasks:
            return None
        t = self.tasks[task_id]
        return f'{{"name":"{t["name"]}","priority":{t["priority"]}}}'

    # ─── Level 2 ───────────────────────────────────────────────────────────────

    def search_tasks(self, timestamp: int, priority: int) -> list[str]:
        matches = [
            (tid, t) for tid, t in self.tasks.items()
            if t["priority"] == priority
        ]
        matches.sort(key=lambda x: -x[1]["order"])
        return [tid for tid, _ in matches]

    def get_tasks_by_priority(self, timestamp: int) -> list[str]:
        sorted_tasks = sorted(
            self.tasks.items(),
            key=lambda x: (x[1]["priority"], x[1]["order"]),
            reverse=True
        )
        return [tid for tid, _ in sorted_tasks]

    # ─── Level 3 ───────────────────────────────────────────────────────────────

    def add_user(self, timestamp: int, user_id: str, quota: int) -> bool:
        if user_id in self.users:
            return False
        self.users[user_id] = {"quota": quota}
        return True

    def update_quota(self, timestamp: int, user_id: str, quota: int) -> bool:
        if user_id not in self.users:
            return False
        self.users[user_id]["quota"] = quota
        return True

    def _active_count(self, timestamp: int, user_id: str) -> int:
        count = 0
        for tid, a in self.assignments.items():
            if a["user_id"] == user_id:
                if tid not in self.completed and a["deadline"] > timestamp:
                    count += 1
        return count

    def assign_task(self, timestamp: int, user_id: str, task_id: str, ttl: int) -> bool:
        if user_id not in self.users or task_id not in self.tasks:
            return False
        # Check if task is already actively assigned
        if task_id in self.assignments:
            a = self.assignments[task_id]
            if task_id not in self.completed and a["deadline"] > timestamp:
                return False
        # Check quota
        if self._active_count(timestamp, user_id) >= self.users[user_id]["quota"]:
            return False
        self.assignments[task_id] = {
            "user_id": user_id,
            "deadline": timestamp + ttl,
            "finish_time": None
        }
        return True

    def finish_task(self, timestamp: int, user_id: str, task_id: str) -> bool:
        if task_id not in self.assignments:
            return False
        a = self.assignments[task_id]
        if a["user_id"] != user_id:
            return False
        if task_id in self.completed:
            return False
        if a["deadline"] <= timestamp:
            return False  # expired
        self.completed.add(task_id)
        a["finish_time"] = timestamp
        return True

    # ─── Level 4 ───────────────────────────────────────────────────────────────

    def get_user_task_history(self, timestamp: int, user_id: str) -> list[str]:
        if user_id not in self.users:
            return []
        history = [
            (tid, a) for tid, a in self.assignments.items()
            if a["user_id"] == user_id
        ]
        history.sort(key=lambda x: (-x[1]["deadline"], x[0]))
        return [tid for tid, _ in history]

    def get_overdue_tasks(self, timestamp: int, user_id: str) -> list[str]:
        if user_id not in self.users:
            return []
        overdue = [
            (tid, a) for tid, a in self.assignments.items()
            if a["user_id"] == user_id
            and a["deadline"] <= timestamp
            and tid not in self.completed
        ]
        overdue.sort(key=lambda x: (-x[1]["deadline"], x[0]))
        return [tid for tid, _ in overdue]