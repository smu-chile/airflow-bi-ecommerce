def truncate_text(text, max_length=1000):
    if text== None:
        return None
    if len(text) > max_length:
        truncated_text = text[:max_length]
        return truncated_text
    else:
        return text
