import re

def bin_label(label, step):
    if label:
        # Extract numeric values
        nums = re.findall(r"-?\d+", label)
        if not nums:
            return "unknown"
        val = int(nums[0])

        if val <= -100:
            return "<= -100"
        elif val >= 100:
            return ">= 100"
        else:
            start = (val // step) * step
            return f"[{start}|{start + step})"
    else:
        return None