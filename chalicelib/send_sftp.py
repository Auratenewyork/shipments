

LoopReportHost = "sftp.custora.com"
LoopReportUsername = "AUrate"
LoopReportPassword = "38683b25a8219d61b98dbeb16"


def send_loop_report(file_like_object):
    import paramiko
    from base64 import decodebytes
    import pysftp

    keydata = b"""AAAAB3NzaC1yc2EAAAADAQABAAABAQCs52CzFGrqis8cybO6Gn8FwDGfWhzpNr6aTc3y5SkkYIYam3BIQwSvu1c7aQqzd+viDJ+2/T+Qs5KlwVvYfHDkhaqSkzGMKojnikhVBVqhLNAiYm6gpMjHOpslywicvP9fGJSy9A5zNakxsPgtDpWsp3yyVOKzjLNmfmubUJd4ISGEPaHFHev4jgqO0LYpAx3qSmVskplx7wiJv8yO/8gWX0q6LmZ/dNhbFngv/Xq1H0gq29k4dz/fy5Oi6Z2+TYpkxQpuIkYBRNuApMQgkaCNtWb9vcNTOcec41iaixs8Wkgmv/SkYKKn0B6g/frUXnDP9s9RxaYhDpBRLXcJ8Yyj"""
    key = paramiko.RSAKey(data=decodebytes(keydata))
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys.add(LoopReportHost, 'ssh-rsa', key)
    with pysftp.Connection(host=LoopReportHost, username=LoopReportUsername,
                           password=LoopReportPassword, cnopts=cnopts) as sftp:
        sftp.cwd('/uploads/')
        file_like_object.seek(0)
        sftp.putfo(file_like_object, './Custora_Order_Items_Returns.csv')