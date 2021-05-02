import time

def size_to_human(size):
    if (size >= 1073741824):
        return "%.1fG" % (size / 1073741824)
    if (size > 1024 * 1024):
        return "%.1fM" % (size / 1048576)
    if (size > 1024):
        return "%.1fK" % (size / 1024)
    return str(int(size))

def time_to_human(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t/1000))

def format_right_text(text, padding=0):
    return ("{:>"+str(padding)+"}").format(text)

def format_time(t, padding=0):
    return format_right_text(time_to_human(t), padding)

def format_human_size(size, padding=0):
    return format_right_text(size_to_human(size), padding)

def _format_size(size, padding=0):
    return format_right_text(str(size), padding)

def _format_text(text, padding=0):
    return text

class Table:

    def __init__(self, *formats):
        self.formats = formats
        self.rows = []

    def append(self, *row):
        self.rows.append(row)

    def print(self):
        formatters = [self._formatter_lookup(f) for f in self.formats]
        sizes = [0 for _ in self.formats]

        for row in self.rows:
            for (ix, formatter) in enumerate(formatters):
                if formatter:
                    cell = formatter(row[ix], 0)
                    sizes[ix] = max(sizes[ix], len(cell))

        for row in self.rows:
            cells = []
            for (ix, formatter) in enumerate(formatters):
                if formatter:
                    cells.append(formatter(row[ix], sizes[ix]))
            print(" ".join(cells))

    def _formatter_lookup(self, format):
        if format is None:
            return None
        if format == "size":
            return _format_size
        if format == "human_size":
            return format_human_size
        if format == "text":
            return _format_text
        if format == "right_text":
            return format_right_text
        if format == "time":
            return format_time
        raise Exception(f"No formatter available for type {format}")
