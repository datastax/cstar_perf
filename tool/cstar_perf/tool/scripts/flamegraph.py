import os
import sh


def setup(flamegraph_directory, flamegraph_path, perf_map_agent_path, java_home):
    """Setup deps for flamegraph"""

    # Create the flamegraph directory and clean the directory
    if not os.path.exists(flamegraph_directory):
        os.mkdir(flamegraph_directory)
    for f in os.listdir(flamegraph_directory):
        file_path = os.path.join(flamegraph_directory, f)
        sh.sudo.rm(file_path)

    if not os.path.exists(perf_map_agent_path):
        sh.git('clone', 'https://github.com/jrudolph/perf-map-agent', perf_map_agent_path)
        sh.cmake('.', _cwd=perf_map_agent_path, _env={'JAVA_HOME': java_home})
        sh.make(_cwd=perf_map_agent_path)

    if not os.path.exists(flamegraph_path):
        sh.git('clone', 'https://github.com/brendangregg/FlameGraph', flamegraph_path)


def ensure_stopped_perf_agent():
    "Ensure there are no perf agent running"

    def try_kill(process_line):
        try:
            sh.sudo.pkill('-f', '-9', process_line)
        except (sh.ErrorReturnCode, sh.SignalException):
            pass

    for p in ['perf.script', 'perf.record', 'perf-java-flames']:
        try_kill(p)


def main():
    """Call function with its parameters"""

    print {function}({parameters})


if __name__ == "__main__":
    main()
