
import argparse
import os
import sys

import radical.entk  as re
import radical.pilot as rp
import radical.utils as ru

# set env variables
os.environ['RADICAL_LOG_LVL']     = 'DEBUG'
os.environ['RADICAL_PROFILE']     = 'TRUE'
os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://rct:rct_test@' \
                                    'apps.marble.ccs.ornl.gov:32020/rct_test'

SMT = os.environ.get('RADICAL_SMT', 4)
CPUS_PER_NODE = 42 * SMT
GPUS_PER_NODE = 6

RESOURCE_DESCRIPTION = {
    'resource'     : 'ornl.summit_jsrun',
    'project'      : 'CSC299',
    'queue'        : 'debug',
    'walltime'     : 120,
    'cpus'         : CPUS_PER_NODE,
    'gpus'         : GPUS_PER_NODE,
    'access_schema': 'local'
}

ENV_SCRIPT    = '/gpfs/alpine/proj-shared/csc299/ckelly/' \
                'chimbuko_10_18_21/spack/load_env.sh'
# gcc4.8.5  tau~mpi install
ENV_SCRIPT_EL = '/gpfs/alpine/proj-shared/csc299/ckelly/' \
                'chimbuko_10_8_21_gcc7_exalearn/load_env.sh'

# ------------------------------------------------------------------------------


def get_args():
    """
    Get arguments.
    :return: Arguments namespace.
    :rtype: _AttributeHolder
    """
    parser = argparse.ArgumentParser(description='Run MOCU using RADICAL-EnTK')
    parser.add_argument('--work_dir', '-o', type=str, required=True,
                        help='work directory')
    parser.add_argument('--num_sim', '-n', type=int, required=True,
                        help='number of simulations')
    parser.add_argument('--nodes', '-r', type=int, required=True,
                        help='number of nodes (NOTE: minimum 2. '
                             '1 node is used for services)')

    return parser.parse_args(sys.argv[1:])

# ------------------------------------------------------------------------------


class MOCU:

    def __init__(self, **kwargs):

        self.work_dir = kwargs.get('work_dir', '.')
        self.num_sim  = kwargs.get('num_sim', 1)

        self._amgr = re.AppManager()
        self._set_resource_description(**kwargs)

    def _set_resource_description(self, **kwargs):

        nodes = kwargs.get('nodes', 1)
        if nodes < 2:
            print('Needs minimum of 2 nodes')
            sys.exit(-1)

        rd = dict(RESOURCE_DESCRIPTION)
        rd['cpus'] *= nodes
        rd['gpus'] *= nodes

        self._amgr.resource_desc = rd
        # !!! NOT DEVELOPED YET !!!
        self._amgr.services = [
            re.Task({
                'uid'       : ru.generate_id('service'),
                'executable': '%s/launch_services.sh' % self.work_dir,
                'pre_exec'  : ['cd %s' % self.work_dir,
                               '.  %s' % ENV_SCRIPT],
                'cpu_reqs'  : {'cpu_processes'  : 1,
                               'cpu_threads'    : CPUS_PER_NODE,
                               'cpu_thread_type': rp.OpenMP}
            })
        ]

    def _get_pipeline(self):

        p = re.Pipeline()

        s = re.Stage()
        for i in range(self.num_sim):
            s.add_tasks(re.Task({
                'executable': '%s/launch_chimbuko_MOCU.sh' % self.work_dir,
                'arguments' : ['%s/chimbuko_config.sh' % self.work_dir,
                               i,
                               self.work_dir,
                               self.num_sim],
                'pre_exec'  : ['. %s' % ENV_SCRIPT_EL],
                'cpu_reqs'  : {'cpu_processes'   : 1,
                               'cpu_threads'     : SMT,
                               'cpu_threads_type': rp.OpenMP},
                'gpu_reqs'  : {'gpu_processes'   : 1,
                               'gpu_process_type': rp.CUDA}
            }))
        p.add_stages(s)

        # shutdown stage
        s = re.Stage()
        s.add_tasks(re.Task({
            'executable': '%s/app_stage_shutdown.sh' % self.work_dir,
            'pre_exec'  : ['cd %s' % self.work_dir,
                           '.  %s' % ENV_SCRIPT,
                           'export CHIMBUKO_CONFIG='
                           '%s/chimbuko_config.sh' % self.work_dir],
            'cpu_reqs'  : {'cpu_processes'   : 1,
                           'cpu_threads'     : SMT,
                           'cpu_threads_type': rp.OpenMP}
        }))
        p.add_stages(s)

        return p

    def run(self):

        self._amgr.workflow = [self._get_pipeline()]
        self._amgr.run()

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    input_options = get_args()
    MOCU(**{'nodes'      : input_options.nodes,
            'work_dir'   : input_options.work_dir,
            'num_sim'    : input_options.num_sim}).run()

# ------------------------------------------------------------------------------

