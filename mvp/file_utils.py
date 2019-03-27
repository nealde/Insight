def handleQuotes(line):
    """This function seeks to handle some inputs which have commas captured by
    quotes as drug names or names with titles - examples include
    "PANCRELIPASE 5,000" and "ABRAHAM, M.D."
    This is done using a boolean, which tracks whether quotes are open or
    closed, and records the indices of commas if quotes are closed.
    Args:
        line (str): The line of a file which has been screened and shown to
            contain at least one quote
    Returns:
        list: A list containing the strings of interest, properly separated."""
    in_quotes = False
    inds = [0]
    for i, c in enumerate(line):
        if c == "," and not in_quotes:
            inds.append(i)
        if c == '"' and not in_quotes:
            in_quotes = True
        elif c == '"' and in_quotes:
            in_quotes = False
    inds.append(len(line) - 1)
    return [line[i + 1: j] for i, j in zip(inds[:-1], inds[1:])]

def clean(line):
    line = line.lower().replace("\n"," ").replace("\r","").replace(">","> ")
    return line


def readLine(line):
    """This function processes and cleans each line of a file.
    Args:
        line (str): Each line of a file after the first line.
            expected_types (list): The type to enforce elementwise. Failure to
            conform to these types will result in a skipped row.
    Returns:
        list: If the data could be read, it is returned in a list."""
#     assert header_index is not None, "Please supply a header index"
    line = clean(line)
    if line.find('"') > 0:
        data = handleQuotes(line)
    else:
        data = line.split("\n")[0].split(",")
    return data
#     try:
#         d = float(data[header_index[4]])
#     except ValueError:
#         print(
#             "%a not able to be cast as float - continuing with value 0"
#             % data[4]
#         )
#         d = 0
#     return (
#         (data[header_index[1]] + data[header_index[2]]).strip(),
#         data[header_index[3]].strip(),
#         d,
# )