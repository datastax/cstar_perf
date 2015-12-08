import os
import sh


def find_process_pid(process_line, child_process=False):
    ps_opts = 'auxww' if not child_process else 'auxfww'
    try:
        pid = sh.awk(
            sh.grep(
                sh.grep(sh.ps(ps_opts, _piped=True, _tty_out=False), "-ie", process_line),
                '-v', 'grep'),
            "{print $2}",
        )
    except sh.ErrorReturnCode:
        raise AssertionError("Cannot find process pid")

    return pid.strip()


def kill_process(pid):
    """Kill a process pid"""
    sh.sudo.kill(pid)


def find_and_kill_process(process_line, child_process=False):
    """Find a process and kill it"""

    pid = find_process_pid(process_line, child_process)
    kill_process(pid)


def clean_directory(directory):
    """Remove all files in a directory"""

    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            sh.rm(f)


def main():
    """Call function with its parameters"""

    print {function}({parameters})


if __name__ == "__main__":
    main()
