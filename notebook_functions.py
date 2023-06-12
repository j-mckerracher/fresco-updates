import pandas as pd


# -------------- CELL 2 --------------
def get_data_files_directory(path) -> str:
    """
    This function should produce a folder path to the data files.
    :param path:
    :return:
    """
    pass


# -------------- CELL 4 --------------
def handle_missing_metrics(starting_time, ending_time, path):
    """
    This function should remove the rows within the given timeframe that are missing metrics.
    :param starting_time:
    :param ending_time:
    :param path:
    :return:
    """
    pass


def add_interval_column(starting_time, ending_time, path):
    """
    This function should add an interval column to the data that falls within the given timeframe. The interval column
    should reflect the length of each timestamp.
    :param starting_time:
    :param ending_time:
    :param path:
    :return:
    """
    pass


# -------------- CELL 5 --------------
node_list = ['NODE1', 'NODE2', 'NODE3', 'NODE4', 'NODE5', 'NODE6', 'NODE7', 'NODE8', 'NODE9', 'NODE10', 'NODE11',
             'NODE12', 'NODE13', 'NODE14', 'NODE15', 'NODE16', 'NODE17', 'NODE18', 'NODE19', 'NODE20', 'NODE21',
             'NODE22', 'NODE23', 'NODE24', 'NODE25', 'NODE26', 'NODE27', 'NODE28', 'NODE29', 'NODE30', 'NODE31',
             'NODE32', 'NODE33', 'NODE34', 'NODE35', 'NODE36', 'NODE37', 'NODE38', 'NODE39', 'NODE40', 'NODE41',
             'NODE42', 'NODE43', 'NODE44', 'NODE45', 'NODE46', 'NODE47', 'NODE48', 'NODE49', 'NODE50', 'NODE51',
             'NODE52', 'NODE53', 'NODE54', 'NODE55', 'NODE58', 'NODE59', 'NODE56', 'NODE57', 'NODE62', 'NODE63',
             'NODE60', 'NODE61', 'NODE67', 'NODE65', 'NODE64', 'NODE66', 'NODE69', 'NODE68', 'NODE70', 'NODE71',
             'NODE73', 'NODE72', 'NODE80', 'NODE77', 'NODE82', 'NODE79', 'NODE74', 'NODE81', 'NODE78', 'NODE75',
             'NODE76', 'NODE84', 'NODE83', 'NODE86', 'NODE85', 'NODE88', 'NODE87', 'NODE89', 'NODE90', 'NODE101',
             'NODE92', 'NODE99', 'NODE98', 'NODE97', 'NODE96', 'NODE103', 'NODE100', 'NODE95', 'NODE102', 'NODE93',
             'NODE94', 'NODE107', 'NODE108', 'NODE111', 'NODE112', 'NODE109', 'NODE110', 'NODE105', 'NODE106',
             'NODE114', 'NODE113', 'NODE115', 'NODE116', 'NODE117', 'NODE118', 'NODE119', 'NODE120', 'NODE121',
             'NODE122', 'NODE123', 'NODE124', 'NODE125', 'NODE126', 'NODE127', 'NODE128', 'NODE131', 'NODE130',
             'NODE136', 'NODE139', 'NODE134', 'NODE137', 'NODE138', 'NODE135', 'NODE132', 'NODE133', 'NODE140',
             'NODE141', 'NODE142', 'NODE150', 'NODE145', 'NODE143', 'NODE149', 'NODE148', 'NODE144', 'NODE147',
             'NODE157', 'NODE146', 'NODE152', 'NODE156', 'NODE154', 'NODE155', 'NODE151', 'NODE153', 'NODE158',
             'NODE161', 'NODE160', 'NODE163', 'NODE164', 'NODE165', 'NODE166', 'NODE162', 'NODE159', 'NODE170',
             'NODE168', 'NODE167', 'NODE169', 'NODE172', 'NODE174', 'NODE173', 'NODE171', 'NODE178', 'NODE176',
             'NODE177', 'NODE175', 'NODE179', 'NODE183', 'NODE180', 'NODE181', 'NODE182', 'NODE190', 'NODE189',
             'NODE188', 'NODE187', 'NODE199', 'NODE196', 'NODE195', 'NODE198', 'NODE197', 'NODE194', 'NODE200',
             'NODE201', 'NODE217', 'NODE210', 'NODE219', 'NODE211', 'NODE218', 'NODE216', 'NODE213', 'NODE212',
             'NODE215', 'NODE214', 'NODE209', 'NODE208', 'NODE220', 'NODE223', 'NODE222', 'NODE221', 'NODE224',
             'NODE226', 'NODE227', 'NODE225', 'NODE233', 'NODE232', 'NODE235', 'NODE234', 'NODE230', 'NODE228',
             'NODE229', 'NODE231', 'NODE236', 'NODE237', 'NODE242', 'NODE245', 'NODE241', 'NODE239', 'NODE240',
             'NODE238', 'NODE244', 'NODE243', 'NODE248', 'NODE247', 'NODE249', 'NODE246', 'NODE250', 'NODE251',
             'NODE252', 'NODE253', 'NODE254', 'NODE255', 'NODE258', 'NODE259', 'NODE256', 'NODE257', 'NODE260',
             'NODE262', 'NODE261', 'NODE263', 'NODE266', 'NODE265', 'NODE264', 'NODE267', 'NODE268', 'NODE271',
             'NODE270', 'NODE269', 'NODE272', 'NODE273', 'NODE275', 'NODE276', 'NODE274', 'NODE278', 'NODE282',
             'NODE280', 'NODE284', 'NODE279', 'NODE286', 'NODE281', 'NODE285', 'NODE283', 'NODE291', 'NODE293',
             'NODE292', 'NODE287', 'NODE290', 'NODE289', 'NODE288', 'NODE294', 'NODE297', 'NODE296', 'NODE295',
             'NODE298', 'NODE303', 'NODE305', 'NODE301', 'NODE306', 'NODE299', 'NODE304', 'NODE300', 'NODE302',
             'NODE307', 'NODE315', 'NODE308', 'NODE313', 'NODE312', 'NODE309', 'NODE310', 'NODE311', 'NODE314',
             'NODE316', 'NODE317', 'NODE318', 'NODE319', 'NODE320', 'NODE321', 'NODE322', 'NODE323', 'NODE324',
             'NODE326', 'NODE327', 'NODE328', 'NODE329', 'NODE330', 'NODE331', 'NODE332', 'NODE333', 'NODE334',
             'NODE335', 'NODE336', 'NODE337', 'NODE345', 'NODE340', 'NODE344', 'NODE341', 'NODE342', 'NODE343',
             'NODE338', 'NODE339', 'NODE354', 'NODE355', 'NODE356', 'NODE361', 'NODE360', 'NODE363', 'NODE359',
             'NODE358', 'NODE357', 'NODE362', 'NODE364', 'NODE365', 'NODE366', 'NODE367', 'NODE368', 'NODE369',
             'NODE373', 'NODE370', 'NODE371', 'NODE372', 'NODE378', 'NODE380', 'NODE377', 'NODE381', 'NODE379',
             'NODE374', 'NODE375', 'NODE376', 'NODE384', 'NODE385', 'NODE386', 'NODE387', 'NODE388', 'NODE389',
             'NODE383', 'NODE382', 'NODE391', 'NODE390', 'NODE395', 'NODE394', 'NODE393', 'NODE392', 'NODE399',
             'NODE397', 'NODE396', 'NODE398', 'NODE400', 'NODE401', 'NODE403', 'NODE402', 'NODE405', 'NODE404',
             'NODE406', 'NODE407', 'NODE408', 'NODE409', 'NODE413', 'NODE410', 'NODE411', 'NODE412', 'NODE414',
             'NODE415', 'NODE416', 'NODE417', 'NODE418', 'NODE419', 'NODE420', 'NODE421', 'NODE422', 'NODE423',
             'NODE424', 'NODE425', 'NODE426', 'NODE427', 'NODE428', 'NODE429', 'NODE347', 'NODE346', 'NODE349',
             'NODE352', 'NODE353', 'NODE348', 'NODE351', 'NODE430', 'NODE431', 'NODE432', 'NODE433', 'NODE434',
             'NODE435', 'NODE436', 'NODE437', 'NODE438', 'NODE439', 'NODE440', 'NODE441', 'NODE442', 'NODE443',
             'NODE444', 'NODE445', 'NODE446', 'NODE447', 'NODE448', 'NODE449', 'NODE450', 'NODE451', 'NODE452',
             'NODE453', 'NODE454', 'NODE455', 'NODE456', 'NODE457', 'NODE458', 'NODE459', 'NODE461', 'NODE460',
             'NODE469', 'NODE468', 'NODE467', 'NODE466', 'NODE465', 'NODE464', 'NODE463', 'NODE462', 'NODE470',
             'NODE471', 'NODE472', 'NODE474', 'NODE473', 'NODE477', 'NODE478', 'NODE479', 'NODE480', 'NODE475',
             'NODE476', 'NODE481', 'NODE482', 'NODE483', 'NODE484', 'NODE485', 'NODE491', 'NODE488', 'NODE492',
             'NODE486', 'NODE490', 'NODE487', 'NODE489', 'NODE493', 'NODE494', 'NODE495', 'NODE498', 'NODE496',
             'NODE499', 'NODE497', 'NODE500', 'NODE501', 'NODE502', 'NODE503', 'NODE504', 'NODE505', 'NODE506',
             'NODE507', 'NODE508', 'NODE509', 'NODE510', 'NODE511', 'NODE512', 'NODE513', 'NODE514', 'NODE516',
             'NODE515', 'NODE517', 'NODE518', 'NODE519', 'NODE520', 'NODE521', 'NODE524', 'NODE523', 'NODE522',
             'NODE526', 'NODE525', 'NODE527', 'NODE528', 'NODE529', 'NODE530', 'NODE531', 'NODE532', 'NODE533',
             'NODE534', 'NODE535', 'NODE536', 'NODE537', 'NODE538', 'NODE539', 'NODE540', 'NODE541', 'NODE542',
             'NODE543', 'NODE544', 'NODE545', 'NODE546', 'NODE547', 'NODE548', 'NODE552', 'NODE549', 'NODE551',
             'NODE550', 'NODE554', 'NODE559', 'NODE556', 'NODE553', 'NODE558', 'NODE555', 'NODE560', 'NODE557',
             'NODE563', 'NODE561', 'NODE562', 'NODE564', 'NODE565', 'NODE568', 'NODE569', 'NODE566', 'NODE567',
             'NODE572', 'NODE571', 'NODE570', 'NODE574', 'NODE580', 'NODE577', 'NODE573', 'NODE575', 'NODE578',
             'NODE579', 'NODE576', 'NODE584', 'NODE583', 'NODE588', 'NODE587', 'NODE585', 'NODE586', 'NODE582',
             'NODE581', 'NODE589', 'NODE592', 'NODE591', 'NODE590', 'NODE593', 'NODE594', 'NODE601', 'NODE602',
             'NODE599', 'NODE600', 'NODE597', 'NODE598', 'NODE595', 'NODE596', 'NODE607', 'NODE608', 'NODE604',
             'NODE603', 'NODE606', 'NODE605', 'NODE609', 'NODE610', 'NODE612', 'NODE611', 'NODE614', 'NODE613',
             'NODE616', 'NODE615', 'NODE617', 'NODE618', 'NODE621', 'NODE620', 'NODE619', 'NODE622', 'NODE624',
             'NODE623', 'NODE625', 'NODE626', 'NODE627', 'NODE628', 'NODE629', 'NODE630', 'NODE632', 'NODE631',
             'NODE633', 'NODE637', 'NODE636', 'NODE635', 'NODE634', 'NODE645', 'NODE644', 'NODE639', 'NODE642',
             'NODE641', 'NODE638', 'NODE643', 'NODE640', 'NODE646', 'NODE647', 'NODE648', 'NODE649', 'NODE651',
             'NODE650', 'NODE654', 'NODE655', 'NODE656', 'NODE657', 'NODE652', 'NODE653', 'NODE658', 'NODE659',
             'NODE660', 'NODE661', 'NODE662', 'NODE663', 'NODE664', 'NODE665', 'NODE666', 'NODE667', 'NODE668',
             'NODE675', 'NODE676', 'NODE673', 'NODE674', 'NODE670', 'NODE669', 'NODE672', 'NODE671', 'NODE678',
             'NODE677', 'NODE679', 'NODE680', 'NODE681', 'NODE682', 'NODE683', 'NODE684', 'NODE685', 'NODE686',
             'NODE687', 'NODE688', 'NODE691', 'NODE689', 'NODE690', 'NODE692', 'NODE693', 'NODE694', 'NODE695',
             'NODE350', 'NODE696', 'NODE697', 'NODE699', 'NODE701', 'NODE704', 'NODE702', 'NODE705', 'NODE706',
             'NODE703', 'NODE700', 'NODE698', 'NODE712', 'NODE714', 'NODE709', 'NODE708', 'NODE711', 'NODE713',
             'NODE710', 'NODE707', 'NODE715', 'NODE716', 'NODE717', 'NODE718', 'NODE719', 'NODE720', 'NODE721',
             'NODE722', 'NODE723', 'NODE733', 'NODE725', 'NODE730', 'NODE731', 'NODE738', 'NODE739', 'NODE727',
             'NODE726', 'NODE729', 'NODE728', 'NODE734', 'NODE737', 'NODE724', 'NODE736', 'NODE735', 'NODE732',
             'NODE740', 'NODE741', 'NODE742', 'NODE744', 'NODE743', 'NODE757', 'NODE754', 'NODE755', 'NODE756',
             'NODE748', 'NODE747', 'NODE746', 'NODE753', 'NODE752', 'NODE749', 'NODE751', 'NODE745', 'NODE750',
             'NODE758', 'NODE759', 'NODE760', 'NODE761', 'NODE762', 'NODE763', 'NODE764', 'NODE765', 'NODE766',
             'NODE767', 'NODE768', 'NODE769', 'NODE772', 'NODE770', 'NODE773', 'NODE771', 'NODE774', 'NODE775',
             'NODE778', 'NODE779', 'NODE782', 'NODE783', 'NODE780', 'NODE781', 'NODE776', 'NODE777', 'NODE784',
             'NODE785', 'NODE786', 'NODE792', 'NODE791', 'NODE797', 'NODE793', 'NODE795', 'NODE790', 'NODE788',
             'NODE798', 'NODE800', 'NODE794', 'NODE796', 'NODE789', 'NODE799', 'NODE787', 'NODE804', 'NODE807',
             'NODE803', 'NODE811', 'NODE810', 'NODE802', 'NODE801', 'NODE805', 'NODE808', 'NODE813', 'NODE806',
             'NODE815', 'NODE812', 'NODE809', 'NODE814', 'NODE818', 'NODE819', 'NODE817', 'NODE816', 'NODE820',
             'NODE822', 'NODE821', 'NODE824', 'NODE823', 'NODE825', 'NODE832', 'NODE830', 'NODE827', 'NODE833',
             'NODE831', 'NODE826', 'NODE829', 'NODE828', 'NODE834', 'NODE835', 'NODE836', 'NODE837', 'NODE838',
             'NODE839', 'NODE840', 'NODE842', 'NODE843', 'NODE841', 'NODE844', 'NODE845', 'NODE846', 'NODE847',
             'NODE848', 'NODE849', 'NODE850', 'NODE851', 'NODE852', 'NODE853', 'NODE854', 'NODE855', 'NODE856',
             'NODE857', 'NODE858', 'NODE859', 'NODE860', 'NODE861', 'NODE862', 'NODE863', 'NODE864', 'NODE865',
             'NODE866', 'NODE867', 'NODE868', 'NODE869', 'NODE870', 'NODE871', 'NODE872', 'NODE873', 'NODE874',
             'NODE875', 'NODE876', 'NODE877', 'NODE878', 'NODE879', 'NODE880', 'NODE881', 'NODE882', 'NODE883',
             'NODE884', 'NODE885', 'NODE886', 'NODE887', 'NODE888', 'NODE889', 'NODE890', 'NODE891', 'NODE892',
             'NODE893', 'NODE894', 'NODE895', 'NODE896', 'NODE897', 'NODE898', 'NODE899', 'NODE900', 'NODE901',
             'NODE902', 'NODE903', 'NODE904', 'NODE905', 'NODE906', 'NODE907', 'NODE908', 'NODE909', 'NODE910',
             'NODE911', 'NODE912', 'NODE913', 'NODE914', 'NODE915', 'NODE916', 'NODE917', 'NODE918', 'NODE919',
             'NODE920', 'NODE921', 'NODE922', 'NODE923', 'NODE924', 'NODE925', 'NODE926', 'NODE927', 'NODE928',
             'NODE929', 'NODE930', 'NODE931', 'NODE932', 'NODE933', 'NODE934', 'NODE935', 'NODE936', 'NODE937',
             'NODE938', 'NODE939', 'NODE940', 'NODE941', 'NODE942', 'NODE943', 'NODE945', 'NODE944', 'NODE946',
             'NODE947', 'NODE948', 'NODE949', 'NODE950', 'NODE951', 'NODE952', 'NODE953', 'NODE954', 'NODE955',
             'NODE325', 'NODE129', 'NODE956', 'NODE957', 'NODE958', 'NODE959', 'NODE91', 'NODE104', 'NODE129',
             'NODE184', 'NODE185', 'NODE186', 'NODE191', 'NODE192', 'NODE193', 'NODE202', 'NODE203', 'NODE204',
             'NODE205', 'NODE206', 'NODE207', 'NODE325']


# -------------- CELL 6 --------------

def get_timeseries_by_timestamp(begin_time: str, end_time: str, return_columns: list) -> pd.DataFrame:
    pass


def get_timeseries_by_values_and_unit(units: str, low_value, high_value) -> pd.DataFrame:
    pass


def get_timeseries_by_hosts(hosts: str) -> pd.DataFrame:
    pass


def get_timeseries_by_job_ids(job_ids: str) -> pd.DataFrame:
    pass


def get_account_logs_by_job_ids(job_ids: str) -> pd.DataFrame:
    pass


# -------------- CELL 7 --------------
def get_average():
    pass


def get_mean():
    pass


def get_median():
    pass


def get_standard_deviation():
    pass


def get_probability_density():
    pass


def get_cumulative_density():
    pass


def get_data_points_outside_threshold():
    pass


def get_ratio_of_data_points_outside_threshold():
    pass


def calculate_correlation():
    pass


# -------------- CELL 8 --------------
# TBD

# -------------- CELL 10 --------------
def get_files():
    pass
