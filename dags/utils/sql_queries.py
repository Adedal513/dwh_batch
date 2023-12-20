HUB_UPDATES = {
"hub_employee": """
INSERT INTO rdv.hub_employee
SELECT DISTINCT
    MD5(p.employeenumber::text),
    now() ,
    'csv',
    p.employeenumber
FROM 
    staging.stg_table p
WHERE p.employeenumber NOT IN (
    SELECT hub.employee_number FROM rdv.hub_employee hub
)
"""
,
"hub_department": """
INSERT INTO rdv.hub_department
SELECT DISTINCT
    MD5(p.department),
    now() ,
    'csv',
    p.department
FROM 
    staging.stg_table p
WHERE p.department NOT IN (
    SELECT hub.department_name FROM rdv.hub_department hub
)
"""
,
"hub_job_role": """
INSERT INTO rdv.hub_job_role
SELECT DISTINCT
    MD5(p.jobrole),
    now() ,
    'csv',
    p.jobrole
FROM 
    staging.stg_table p
WHERE p.jobrole NOT IN (
    SELECT hub.job_role_name FROM rdv.hub_job_role hub
)
"""
}

LINK_UPDATES = {
"lnk_employee_department": """
INSERT INTO rdv.lnk_employee_department
SELECT distinct
	md5(concat(sod.department, sod.employeenumber::text)),
	now() ,
	'csv',
	md5(sod.employeenumber::text),
    md5(sod.department)
FROM
    staging.stg_table sod
WHERE
    NOT EXISTS (
        SELECT
            l.employee_hash_key
        FROM rdv.lnk_employee_department l
        WHERE
            l.employee_hash_key= md5(sod.employeenumber::text)
        AND
            l.department_hash_key = md5(sod.department)
    )
""",
"lnk_employee_job_role_": """
INSERT INTO rdv.lnk_employee_job_role
SELECT distinct
	md5(concat(sod.jobrole, sod.employeenumber::text)),
	now() ,
	'csv',
	md5(sod.employeenumber::text),
    md5(sod.jobrole)
FROM
    staging.stg_table sod
WHERE
    NOT EXISTS (
        SELECT
            l.employee_hash_key
        FROM rdv.lnk_employee_job_role l
        WHERE
            l.employee_hash_key= md5(sod.employeenumber::text)
        AND
            l.job_role_hash_key = md5(sod.jobrole)
    )
"""
}

SATTELITE_UPDATES = {
"sat_employee_history": """
INSERT INTO rdv.sat_employee_history
SELECT DISTINCT
	md5(so.employeenumber::text),
	'csv',
	now() ,
	so.numcompaniesworked,
	so.totalworkingyears,
	so.yearsatcompany,
	so.yearsincurrentrole,
	so.yearssincelastpromotion,
	so.yearsincurrentrole
FROM
    staging.stg_table so
LEFT OUTER JOIN rdv.sat_employee_history sat ON (sat.employee_hash_key = md5(so.employeenumber::text) AND sat.created_at IS NULL)
where 
	coalesce(so.numcompaniesworked, 0) != coalesce(sat.num_companies_worked, 0) or
	coalesce(so.totalworkingyears, 0) != coalesce(sat.total_working_years, 0) or
	coalesce(so.yearsatcompany, 0) != coalesce(sat.years_at_company, 0) or
	coalesce(so.yearsincurrentrole, 0) != coalesce(sat.years_in_current_role, 0) or
	coalesce(so.yearssincelastpromotion, 0) != coalesce(sat.years_since_last_promotion, 0) or
	coalesce(so.yearsincurrentrole, 0) != coalesce(sat.years_with_current_manager, 0)
""",
"sat_employee_job_details": """
    INSERT INTO rdv.sat_employee_job_details
    SELECT DISTINCT
        md5(so.employeenumber::text),
        'csv',
        now() ,
        case when so.attrition = 'Yes' then true else false end,
        so.hourlyrate,
        so.joblevel,
        so.standardhours,
        so.stockoptionlevel
    FROM
        staging.stg_table so
    LEFT OUTER JOIN rdv.sat_employee_job_details sat ON (sat.employee_hash_key = md5(so.employeenumber::text) AND sat.created_at IS NULL)
    where 
        coalesce(case when so.attrition = 'Yes' then true else false end, false) != coalesce(sat.attrition, false) or
        coalesce(so.joblevel, 0) != coalesce(sat.job_level, 0) or
        coalesce(so.hourlyrate, 0) != coalesce(sat.hourly_rate, 0) or
        coalesce(so.standardhours, 0) != coalesce(sat.standard_hours, 0) or
        coalesce(so.stockoptionlevel, 0) != coalesce(sat.stock_option_level, 0)
""",
"sat_employee_personal": """
INSERT INTO rdv.sat_employee_personal
SELECT DISTINCT
	md5(so.employeenumber::text),
	'csv',
	now() ,
	so.name,
	so.lastname,
	so.gender,
	so.age,
	case when so.over18 = 'Y' then true else false end,
	so.maritalstatus,
	so.education,
	so.educationfield,
	so.distancefromhome
FROM
    staging.stg_table so
LEFT OUTER JOIN rdv.sat_employee_personal sat ON (sat.employee_hash_key = md5(so.employeenumber::text) AND sat.created_at IS NULL)
where 
	coalesce(case when so.over18 = 'Y' then true else false end) != coalesce(sat.over_eighteen, false) or
	coalesce(so.name, '') != coalesce(sat.name, '') or
	coalesce(so.lastname, '') != coalesce(sat.lastname, '') or
	coalesce(so.gender, '') != coalesce(sat.gender, '') or
	coalesce(so.age, 0) != coalesce(sat.age, 0) or
	coalesce(so.maritalstatus, '') != coalesce(sat.marital_status, '') or
	coalesce(so.education, 0) != coalesce(sat.education, 0) or
	coalesce(so.educationfield, '') != coalesce(sat.education_field, '') or
	coalesce(so.distancefromhome, 0) != coalesce(sat.distance_field, 0)
""",
"sat_employee_social_performance": """
INSERT INTO rdv.sat_employee_social_performance
SELECT DISTINCT
	md5(so.employeenumber::text),
	'csv',
	now() ,
	so.environmentsatisfaction,
	so.jobsatisfaction,
	so.relationshipsatisfaction,
	so.worklifebalance
FROM
    staging.stg_table so
LEFT OUTER JOIN rdv.sat_employee_social_performance sat ON (sat.employee_hash_key = md5(so.employeenumber::text) AND sat.created_at IS NULL)
where 
	coalesce(so.environmentsatisfaction, 0) != coalesce(sat.environment_satisfaction, 0) or
	coalesce(so.jobsatisfaction, 0) != coalesce(sat.job_satisfaction, 0) or
	coalesce(so.relationshipsatisfaction, 0) != coalesce(sat.relationship_satisfaction, 0) or
	coalesce(so.worklifebalance, 0) != coalesce(sat.work_life_balance, 0)
""",
"sat_employee_working_performance": """
INSERT INTO rdv.sat_employee_working_performance
SELECT DISTINCT
	md5(so.employeenumber::text),
	'csv',
	now() ,
	so.performancerating,
	so.businesstravel,
	so.jobinvolvement,
	case when so.overtime = 'Yes' then true else false end,
	so.trainingtimeslastyear
FROM
    staging.stg_table so
LEFT OUTER JOIN rdv.sat_employee_working_performance sat ON (sat.employee_hash_key = md5(so.employeenumber::text) AND sat.created_at IS NULL)
where 
	coalesce(so.performancerating, 0) != coalesce(sat.performance_rating, 0) or
	coalesce(so.businesstravel, '') != coalesce(sat.business_travel, '') or
	coalesce(so.jobinvolvement, 0) != coalesce(sat.job_involvement, 0) or
	coalesce(case when so.overtime = 'Yes' then true else false end, false) != coalesce(sat.overtime, false) or
	coalesce(so.trainingtimeslastyear, 0) != coalesce(sat.training_times_last_year, 0)
"""
}