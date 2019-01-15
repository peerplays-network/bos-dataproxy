from urllib.parse import urlsplit, parse_qs


def get_params(req, *args):
    params = parse_qs(urlsplit(req.url).query)

    for key in args:
        if params.get(key, None) is None:
            params[key] = None
        elif type(params[key]) == list and len(params[key]) == 1:
            params[key] = params[key][0]

        if params[key] is not None and "," in params[key]:
            params[key] = params[key].split(",")

    return params
