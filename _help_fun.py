from flask import flash


def flash_mess(mess):
    flash(mess, category='error')


def message_former_from(message_dict):
    messages = []
    for mess_name, mess in message_dict.items():
        mess_str = ', '.join(mess)
        if mess_str:
            messages.append(': '.join([mess_name, mess_str]) + '.')
        else:
            messages.append(mess_name + '.')

    return '\n'.join(messages)


