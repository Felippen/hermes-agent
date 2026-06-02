import base64


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8").rstrip("=")


class FakeCall:
    def __init__(self, value):
        self.value = value

    def execute(self):
        return self.value


class FakeMessages:
    def __init__(self, service):
        self.service = service

    def list(self, **kwargs):
        self.service.list_calls.append(kwargs)
        if self.service.pages is not None:
            page_token = kwargs.get("pageToken") or ""
            ids, next_page_token = self.service.pages.get(page_token, ([], ""))
            return FakeCall({
                "messages": [{"id": key} for key in ids],
                "nextPageToken": next_page_token,
            })
        if kwargs.get("pageToken"):
            return FakeCall({"messages": []})
        return FakeCall({
            "messages": [{"id": key} for key in self.service.messages.keys()],
            "nextPageToken": "next-page",
        })

    def get(self, **kwargs):
        self.service.get_calls.append(kwargs)
        return FakeCall(self.service.messages[kwargs["id"]])

    def send(self, **kwargs):
        self.service.send_calls.append(kwargs)
        return FakeCall({
            "id": "sent-1",
            "threadId": kwargs["body"].get("threadId", "thread-sent"),
        })

    def modify(self, **kwargs):
        self.service.modify_calls.append(kwargs)
        return FakeCall({
            "id": kwargs["id"],
            "labelIds": kwargs["body"].get("addLabelIds", []),
        })

    def trash(self, **kwargs):
        self.service.trash_calls.append(kwargs)
        return FakeCall({"id": kwargs["id"], "labelIds": ["TRASH"]})


class FakeUsers:
    def __init__(self, service):
        self.service = service

    def messages(self):
        return FakeMessages(self.service)


class FakeGmailService:
    def __init__(self, messages, pages=None):
        self.messages = messages
        self.pages = pages
        self.list_calls = []
        self.get_calls = []
        self.send_calls = []
        self.modify_calls = []
        self.trash_calls = []

    def users(self):
        return FakeUsers(self)


def _message(message_id="msg-1", labels=None):
    return {
        "id": message_id,
        "threadId": "thread-1",
        "snippet": "hello snippet",
        "labelIds": labels or ["INBOX", "UNREAD", "STARRED"],
        "payload": {
            "headers": [
                {"name": "From", "value": "sender@example.com"},
                {"name": "To", "value": "user@example.com"},
                {"name": "Cc", "value": "cc@example.com"},
                {"name": "Subject", "value": "Status"},
                {"name": "Date", "value": "Mon, 01 Jun 2026 10:00:00 +0000"},
                {"name": "Message-ID", "value": "<msg-1@example.com>"},
            ],
            "parts": [
                {
                    "mimeType": "text/plain",
                    "body": {"data": _b64("Plain body")},
                },
                {
                    "mimeType": "text/html",
                    "body": {"data": _b64("<p>HTML body</p>")},
                },
                {
                    "filename": "report.pdf",
                    "mimeType": "application/pdf",
                    "body": {"attachmentId": "att-1", "size": 123},
                },
            ],
        },
    }
