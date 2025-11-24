from collections import OrderedDict


class FixedSizeDict:
    def __init__(self, max_size):
        """
        初始化一个固定大小的字典缓存。
        
        :param max_size: 字典的最大容量
        """
        self.max_size = max_size
        self.cache = OrderedDict()

    def __setitem__(self, key, value):
        """
        插入或更新键值对。
        如果字典已满，则移除最早插入的键值对。
        """
        if key in self.cache:
            # 如果键已存在，先将其移到末尾（表示最近使用）
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            # 移除最早插入的键值对
            self.cache.popitem(last=False)

    def __getitem__(self, key):
        """
        获取键对应的值，并将该键移到末尾（表示最近使用）。
        """
        value = self.cache[key]
        self.cache.move_to_end(key)
        return value

    def __contains__(self, key):
        """
        检查键是否存在于缓存中。
        """
        return key in self.cache

    def __len__(self):
        """
        返回缓存中的键值对数量。
        """
        return len(self.cache)

    def __repr__(self):
        """
        返回缓存的字符串表示。
        """
        return repr(self.cache)

# 示例用法
if __name__ == "__main__":
    cache = FixedSizeDict(max_size=5)

    # 插入数据
    cache["a"] = 1
    cache["b"] = 2
    cache["c"] = 3
    cache["d"] = 4
    cache["e"] = 5

    print("当前缓存:", cache)

    # 插入第6个元素，移除最早的元素
    cache["f"] = 6
    print("插入第6个元素后:", cache)

    # 访问某个元素（会将其移到末尾）
    print("访问 'b':", cache["b"])
    print("访问后缓存:", cache)

    # 检查键是否存在
    print("'a' 是否在缓存中:", "a" in cache)