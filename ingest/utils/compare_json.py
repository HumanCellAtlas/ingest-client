# source: https://gist.github.com/magnetikonline/845400198a8e4e4648746a675e955af3


def compare_json_data(source_data_a, source_data_b):
    def compare(data_a, data_b):
        # type : list
        if type(data_a) is list:
            # is [data_b] a list and of same length as [data_a]?
            if (
                    (type(data_b) != list) or
                    (len(data_a) != len(data_b))
            ):
                return False

            # iterate over list items
            for list_index, list_item in enumerate(data_a):
                # compare [data_a] list item against [data_b] at index
                if not compare(list_item, data_b[list_index]):
                    return False

            # list identical
            return True

        # type : dictionary
        if type(data_a) is dict:
            # is [data_b] a dictionary?
            if type(data_b) != dict:
                return False

            # iterate over dictionary keys
            for dict_key, dict_value in data_a.items():
                # key exists in [data_b] dictionary, and same value?
                if (
                        (dict_key not in data_b) or
                        (not compare(dict_value, data_b[dict_key]))
                ):
                    return False

            # dictionary identical
            return True

        # simple value - compare both value and type for equality
        return (
                (data_a == data_b) and
                (type(data_a) is type(data_b))
        )

    # compare a to b, then b to a
    return (
            compare(source_data_a, source_data_b) and
            compare(source_data_b, source_data_a)
    )
