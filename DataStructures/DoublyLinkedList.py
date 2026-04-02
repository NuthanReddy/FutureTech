from __future__ import annotations

from typing import Any, Optional


class DNode:
    """Node for a doubly linked list."""

    def __init__(self, data: Any, prev_node: Optional["DNode"] = None,
                 next_node: Optional["DNode"] = None) -> None:
        self.data = data
        self.prev = prev_node
        self.next = next_node

    def __str__(self) -> str:
        return str(self.data)


class DoublyLinkedList:
    """Doubly linked list with head/tail pointers.

    Supports O(1) append/prepend and O(1) removal from both ends.
    """

    def __init__(self) -> None:
        self.head: Optional[DNode] = None
        self.tail: Optional[DNode] = None
        self.length: int = 0

    # ---- insertion ----

    def append(self, data: Any) -> "DoublyLinkedList":
        """Insert at the tail."""
        node = DNode(data)
        if self.tail is None:
            self.head = self.tail = node
        else:
            node.prev = self.tail
            self.tail.next = node
            self.tail = node
        self.length += 1
        return self

    def prepend(self, data: Any) -> "DoublyLinkedList":
        """Insert at the head."""
        node = DNode(data)
        if self.head is None:
            self.head = self.tail = node
        else:
            node.next = self.head
            self.head.prev = node
            self.head = node
        self.length += 1
        return self

    # ---- deletion ----

    def delete_head(self) -> Any:
        if self.head is None:
            raise IndexError("delete from empty list")
        data = self.head.data
        if self.head is self.tail:
            self.head = self.tail = None
        else:
            self.head = self.head.next
            self.head.prev = None
        self.length -= 1
        return data

    def delete_tail(self) -> Any:
        if self.tail is None:
            raise IndexError("delete from empty list")
        data = self.tail.data
        if self.head is self.tail:
            self.head = self.tail = None
        else:
            self.tail = self.tail.prev
            self.tail.next = None
        self.length -= 1
        return data

    # ---- traversal helpers ----

    def to_list(self) -> list:
        """Return elements as a plain Python list (head → tail)."""
        result = []
        curr = self.head
        while curr is not None:
            result.append(curr.data)
            curr = curr.next
        return result

    def to_list_reverse(self) -> list:
        """Return elements as a plain Python list (tail → head)."""
        result = []
        curr = self.tail
        while curr is not None:
            result.append(curr.data)
            curr = curr.prev
        return result

    def __len__(self) -> int:
        return self.length

    def __str__(self) -> str:
        return " <-> ".join(str(d) for d in self.to_list())

    def __iter__(self):
        curr = self.head
        while curr is not None:
            yield curr.data
            curr = curr.next


if __name__ == "__main__":
    dll = DoublyLinkedList()
    dll.append(1).append(2).append(3)
    dll.prepend(0)
    print(dll)                    # 0 <-> 1 <-> 2 <-> 3
    print(dll.delete_tail())      # 3
    print(dll.delete_head())      # 0
    print(dll)                    # 1 <-> 2

