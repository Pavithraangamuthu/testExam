import logging

import azure.functions as func


def main(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    input_msg = req.params.get('message')

    if not input_msg:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('message')

    if input_msg:
         msg.set(input_msg)
    else:
         msg.set("Container completed image processing")

    return 'OK'
