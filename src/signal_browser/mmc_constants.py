import enum


class TDDW(enum.IntEnum):
    TDDW_TripIn = 1
    TDDW_TripOut = 2
    TDDW_AddStand = 3
    TDDW_TakeWeight = 101
    TDDW_SetWeight = 102
    TDDW_Latch = 105
    TDDW_Unlatch = 106
    TDDW_Connect = 107
    TDDW_Disconnect = 108
    TDDW_FillPipe_TripIn = 109
    TDDW_PumpOut = 110
    TDDW_SetWeightStickup = 113
    TDDW_TakeWeightHandover =116
    TDDW_LatchHhandover = 118
    TDDW_Unlatchtickup = 119
    TDDW_GetHandover = 121


class HR(enum.IntEnum):
    HR_TripIn = 1
    HR_TripOut = 2
    HR_AddStand = 3
    HR_GetStandfromFB = 101
    HR_PutStandinFB = 102
    HR_Connect_RBS = 105
    WTF = 106

class HT(enum.IntEnum):
    HT_TripIn = 1
    HT_TripOut = 2
    HT_AddStand = 3
    HT_PrepareforConnection = 101
    HT_DrainPipe = 105
    HT_Connect = 106
    HT_Disconnect = 107
    HT_TorqueUp = 108
    HT_BreakConnection = 109
    HT_Spin = 110

class EBT(enum.IntEnum):
    EBT_AddStand = 3


class Machine_ID(enum.IntEnum):
    TDDW = 2
    HT = 5
    EBT = 6
    RT = 7
    PSL = 8
    GA1 = 9
    GA2 = 10
    VPC = 11
    PTA = 12
    UGA = 15
    LCY = 18
    RCW = 19
    SbHT = 20
    SbGA = 21
    MH1 = 23
    MH2 = 22
    PriHR = 63
    SecHR = 64