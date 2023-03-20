
import argparse
import os
import sys

import radical.pilot as rp

# set env variables
os.environ['RADICAL_LOG_LVL']     = 'DEBUG'
os.environ['RADICAL_PROFILE']     = 'TRUE'
os.environ['RADICAL_PILOT_DBURL'] = 'mongodb://rct:rct_test@' \
                                    'apps.marble.ccs.ornl.gov:32020/rct_test'

SMT = os.environ.get('RADICAL_SMT', 4)
CPUS_PER_NODE = 42 * SMT
GPUS_PER_NODE = 6

PILOT_DESCRIPTION = {
    'resource'     : 'ornl.summit_jsrun',
    'project'      : 'CSC299',
    'queue'        : 'debug',
    'runtime'      : 120,
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
    parser = argparse.ArgumentParser(description='Run MOCU using RADICAL-Pilot')
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

        pd = self._get_pilot_description(**kwargs)

        self._session = rp.Session()

        pmgr  = rp.PilotManager(session=self._session)
        pilot = pmgr.submit_pilots(rp.PilotDescription(pd))

        self._tmgr = rp.TaskManager(session=self._session)
        self._tmgr.add_pilots(pilot)

    def _get_pilot_description(self, **kwargs):

        nodes = kwargs.get('nodes', 1)
        if nodes < 2:
            print('Needs minimum of 2 nodes')
            sys.exit(-1)

        pd = dict(PILOT_DESCRIPTION)
        pd.update({
            'nodes'   : nodes,
            'services': [
                rp.TaskDescription({
                    'executable'    : '%s/launch_services.sh' % self.work_dir,
                    'pre_exec'      : ['cd %s' % self.work_dir,
                                       '.  %s' % ENV_SCRIPT],
                    'ranks'         : 1,
                    'cores_per_rank': CPUS_PER_NODE,
                    'threading_type': rp.OpenMP
                })
            ]
        })

        return pd

    def _get_task_descriptions_main(self):

        tds = []
        for i in range(self.num_sim):

            tds.append(rp.TaskDescription({
                'executable'    : '%s/launch_chimbuko_MOCU.sh' % self.work_dir,
                'arguments'     : ['%s/chimbuko_config.sh' % self.work_dir,
                                   i,
                                   self.work_dir,
                                   self.num_sim],
                'pre_exec'      : ['. %s' % ENV_SCRIPT_EL],
                'ranks'         : 1,
                'cores_per_rank': SMT,
                'threading_type': rp.OpenMP,
                'gpus_per_rank' : 1,
                'gpu_type'      : rp.CUDA
            }))

        return tds

    def _get_task_description_final(self):

        return rp.TaskDescription({
            'executable'    : '%s/app_stage_shutdown.sh' % self.work_dir,
            'pre_exec'      : ['cd %s' % self.work_dir,
                               '.  %s' % ENV_SCRIPT,
                               'export CHIMBUKO_CONFIG='
                               '%s/chimbuko_config.sh' % self.work_dir],
            'ranks'         : 1,
            'cores_per_rank': SMT,
            'threading_type': rp.OpenMP
        })

    def run(self):

        self._tmgr.submit_tasks(self._get_task_descriptions_main())
        self._tmgr.wait_tasks()

        self._tmgr.submit_tasks(self._get_task_description_final())
        self._tmgr.wait_tasks()

        self._session.close(download=True)

# ------------------------------------------------------------------------------


if __name__ == '__main__':

    input_options = get_args()
    MOCU(**{'nodes'      : input_options.nodes,
            'work_dir'   : input_options.work_dir,
            'num_sim'    : input_options.num_sim}).run()

# ------------------------------------------------------------------------------

