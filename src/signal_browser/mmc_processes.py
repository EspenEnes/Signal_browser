import zipfile
from .utils import retain_changed_values_on_series
from .mmc_constants import Machine_ID, TDDW, HR, HT, EBT
import pandas as pd
import plotly.express as px

SEQ_COLUMNS_BATCHES = [
    [1, 3, 5, 7, 9],
    [12, 14, 16, 18, 20],
    [23, 25, 27, 29, 31],
    [34, 36, 38, 40, 42],
    [45, 47, 49, 51, 53],
]

STEP_COLUMNS_BATCHES = [
    [2, 4, 6, 8, 10],
    [13, 15, 17, 19, 21],
    [24, 26, 28, 30, 32],
    [35, 37, 39, 41, 43],
    [46, 48, 50, 52, 54],
]

TOOL_COLUMNS = [0, 11, 22, 33, 44]


class MMCProcesses:
    @classmethod
    def make_plotly_figure(cls, data_frame):
        """
        Generates a Plotly figure object based on the given Seq and Step columns.
        """
        tasks = cls.generate_tasks(
            data_frame=data_frame,
            seq_columns=SEQ_COLUMNS_BATCHES,
            step_columns=STEP_COLUMNS_BATCHES,
            tool_columns=TOOL_COLUMNS,
        )

        if tasks.empty:
            return {}

        fig = px.timeline(
            tasks,
            x_start="start",
            x_end="end",
            y="seq",
            color="group",
            opacity=0.9,
            hover_data="step",
            custom_data="width",
        )

        for i, d in enumerate(fig.data):
            d.width = tasks[tasks["group"] == d.name]["width"]

        return fig

    @classmethod
    def generate_tasks(
        cls,
        data_frame: pd.DataFrame | zipfile.ZipFile,
        seq_columns: list[int] | list[list[int]],
        step_columns: list[int] | list[list[int]],
        tool_columns: list[int] | None = None,
        ids: list[int] | None = None,
    ):
        """Generate tasks for each machine in the groups list, each tool can have 5 different slots and each slot can run different sequences, the sequenses again can have different steps."""
        groups = cls.define_groups(tool_columns, ids, data_frame)

        if isinstance(seq_columns[0], list):
            if len(seq_columns) == len(groups) and (len(step_columns) == len(groups)):
                return cls._generate_tasks_for_batches(groups, seq_columns, step_columns, data_frame)
            else:
                raise Exception("Length of seq_col, seq_step do not match with length of groups")

        elif isinstance(seq_columns[0], int):
            if len(groups) == 1:
                return cls._generate_tasks_single_machine(groups[0], seq_columns, step_columns, data_frame)
            else:
                raise Exception("Length of seq_col, seq_step do not match with length of groups")
        return

    @classmethod
    def _generate_tasks_for_batches(cls, machine_ids, sequence_columns_batches, step_columns_batches, data_frame):
        if len(sequence_columns_batches) != len(machine_ids) or len(step_columns_batches) != len(machine_ids):
            raise Exception(
                "Length of sequence_columns_batches, step_columns_batches do not match with length of groups"
            )

        output = []
        for sequence_columns, step_columns, machine_id in zip(
            sequence_columns_batches, step_columns_batches, machine_ids
        ):
            machine_tasks = cls._generate_tasks_single_machine(machine_id, sequence_columns, step_columns, data_frame)
            output.append(machine_tasks)

        return pd.concat(output)

    @classmethod
    def _generate_tasks_single_machine(cls, machine_id, seq_columns, step_columns, data_frame):
        seq_pair = []
        seq_column_data = data_frame.iloc[:, seq_columns]
        step_column_data = data_frame.iloc[:, step_columns]
        for seq, step in zip(seq_column_data, step_column_data):
            new = [seq_column_data[seq], step_column_data[step]]
            seq_pair.append(new)
        machine_tasks = cls._generate_machine_tasks(machine_id, seq_pair)
        return machine_tasks

    @classmethod
    def _generate_machine_tasks(cls, machine_id, seq_pair):
        """Slot Sequense_ID, and Slot Step_ID as inputs
        This function generates tasks from the 5 different slots that MMC can use pr machine,
        each. each task has different active steps that is stored in another DB"""
        output = []

        # loop over seq_id, seq_step pairs
        for df_seq, df_steps in seq_pair:
            tasks = cls._find_start_stop_of_sequenses(machine_id, df_seq, df_steps)
            output.extend(tasks)

        return pd.DataFrame(output)

    @classmethod
    def _find_start_stop_of_sequenses(cls, machine_id, df_seq, df_steps):
        output = []
        old = None
        start = None
        df = retain_changed_values_on_series(df_seq)
        for timestamp, seq_id in df.dropna().items():
            if old != seq_id:
                has_changes = True
            else:
                has_changes = False

            if not start and has_changes:
                if seq_id > 0:
                    """If new task and no task started, start new task"""
                    start = timestamp
                    end = None
                    seq_id = seq_id
            elif start and has_changes:
                """If new task and a task is already started, end task"""
                end = timestamp

                if seq_id > 0:
                    """If task was ended because a new task was forced in, start new task"""
                    tasks = cls._find_start_stop_of_steps(machine_id, df_steps, seq_id, start, end)
                    output.extend(tasks)
                    start = timestamp
                    seq_id = seq_id
                    end = None
                elif seq_id == 0:
                    tasks = cls._find_start_stop_of_steps(machine_id, df_steps, old, start, end)
                    output.extend(tasks)
                    start = None
                    seq_id = None
                    end = None
            old = seq_id
        return output

    @classmethod
    def _find_start_stop_of_steps(cls, machine_id, df: pd.Series, seq_id, start_dt, end_dt):
        machine_name = cls.get_machine_name(machine_id)
        seq_name = cls.get_sequence_name(machine_id, seq_id)

        output = []
        start = None
        end = None
        step = None

        mask = (df.index >= start_dt) & (df.index <= end_dt)
        new_df = df.where(mask).dropna()

        new_df = retain_changed_values_on_series(new_df)

        old = None

        for timestamp, value in new_df.items():
            if old != value:
                has_changes = True
            else:
                has_changes = False

            if not start and has_changes:
                if value > 0:
                    """If new task and no task started, start new task"""
                    start = timestamp
                    end = None
                    step = value
            elif start and has_changes:
                """If new task and a task is already started, end task"""
                end = timestamp
                cls.add_task(output, seq_name, machine_name, start, end, step)

                if value > 0:
                    """If task was ended because a new task was forced in, start new task"""
                    start = timestamp
                    step = value
                    end = None
                elif value == 0:
                    start = None
                    step = None
                    end = None
            old = value

        if start and not end:
            """If task is not ended on last iteration of the data, end task"""
            end = new_df.index[-1]
            cls.add_task(output, seq_name, machine_name, start, end, step)

        return output

    @classmethod
    def add_task(cls, output, seq_name, machine_name, start, end, step):
        output.append(dict(seq=seq_name, step=step, start=start, end=end, width=0.1 + (step / 100), group=machine_name))

    @classmethod
    def get_machine_name(cls, machine_id):
        try:
            return Machine_ID(machine_id).name
        except:
            return f"Tool[{int(machine_id)}]"

    @classmethod
    def get_sequence_name(cls, machine_id, task):
        seq_enum = cls.select_sequence_enum(machine_id)
        try:
            if seq_enum:
                return seq_enum(int(task)).name
        except ValueError:
            pass

        machine_name = cls.get_machine_name(machine_id)
        return f"{machine_name}:Seq[{int(task)}]"

    @classmethod
    def select_sequence_enum(cls, machine_id):
        try:
            if Machine_ID(machine_id) == Machine_ID.TDDW:
                return TDDW
            elif Machine_ID(machine_id) == Machine_ID.PriHR:
                return HR
            elif Machine_ID(machine_id) == Machine_ID.HT:
                return HT
            elif Machine_ID(machine_id) == Machine_ID.EBT:
                return EBT
            else:
                return None
        except NameError:
            return None

    @classmethod
    def define_groups(cls, tool_columns, ids, data_frame):
        """

        Defines groups based on the given parameters.

        :param tool_columns: A list of column indices in the `data_frame` representing the tool values.
        :param ids: A list of group ids.
        :param data_frame: A pandas DataFrame containing the data.

        :returns: A list of group ids.

        :raises Exception: If both `ids` and `tool_columns` are provided or if neither is provided.

        """
        if ids and not tool_columns:
            return ids
        elif tool_columns and not ids:
            tool_values = data_frame.iloc[:, tool_columns].dropna().iloc[0].values
            return [int(x) for x in tool_values]
        else:
            raise Exception("Only one way to define groups can be used.")
