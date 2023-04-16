/*
Covid 19 Data Exploration 

Skills used: Joins,Stored Procedure, CTE, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types
etc
*/

use my_portfolio_project;

/* created a manual table using LOAD DATA INFILE for 
fast importing of millions of datas
*/
create table CovidDeaths (iso_code varchar(255), continent varchar(255), location varchar(255), 
date date, population bigint default null,total_cases text null , new_cases text null, 
new_cases_smoothed text null,total_deaths text  null, new_deaths text null, 
new_deaths_smoothed text null, total_cases_per_million text null, 
new_cases_per_million text null,
new_cases_smoothed_per_million text null, total_deaths_per_million text null,
 new_deaths_per_million text null,
new_deaths_smoothed_per_million text null, reproduction_rate text null, 
icu_patients text null, 
icu_patients_per_million text null, hosp_patients text null,
 hosp_patients_per_million text null,
weekly_icu_admissions text null, weekly_ice_admissions_per_million text null,
weekly_hosp_admissions text null, weekly_hosp_admissions_per_million text null);

select * from coviddeaths;

load data infile 'CovidDeaths.csv'
into table coviddeaths
fields terminated by ','
lines terminated by '\n'
ignore 1 lines;

create table CovidVaccinations(iso_code varchar(255),continent varchar(255), location varchar(255),date	date,
	total_tests text null,new_tests text null,total_tests_per_thousand text null,
    new_tests_per_thousand text null,new_tests_smoothed text null,
    new_tests_smoothed_per_thousand text null,positive_rate text null,	tests_per_case text null,	tests_units text null,
	total_vaccinations text null,	people_vaccinated text null,	people_fully_vaccinated text null,	
    total_boosters text null,	new_vaccinations text null,	new_vaccinations_smoothed text null,	total_vaccinations_per_hundred text null,
	people_vaccinated_per_hundred text null,	people_fully_vaccinated_per_hundred text null,	total_boosters_per_hundred text null,
	new_vaccinations_smoothed_per_million text null,	new_people_vaccinated_smoothed text null,	new_people_vaccinated_smoothed_per_hundred text null,
	stringency_index text null,	population_density text null,	median_age text null,	aged_65_older text null,	aged_70_older text null,
	gdp_per_capita double null,	extreme_poverty text null,	cardiovasc_death_rate text null,	diabetes_prevalence text null,	
    female_smokers text null,	male_smokers text null,	handwashing_facilities text null,
	hospital_beds_per_thousand text null,	life_expectancy text null,
	human_development_index text null,	excess_mortality_cumulative_absolute text null,	excess_mortality_cumulative text null,
	excess_mortality text null,	excess_mortality_cumulative_per_million text null);

select * from covidvaccinations;

load data infile 'CovidVaccinations.csv'
into table covidvaccinations
fields terminated by ','
lines terminated by '\n'
ignore 1 lines;

#alter the table to change data type of a column
alter table covidvaccinations
modify column gdp_per_capita text null;

select * from my_portfolio_project.coviddeaths
order by 3,4;

#select * from my_portfolio_project.covidvaccinations
#order by 3,4;

select location,date,total_cases,new_cases,total_deaths,
population from coviddeaths 
order by 1,2;

/*total cases vs total deaths
#shows the percentage of dying in india if you are corona positive*/

select location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 as death_percentage
from coviddeaths 
where location like '%india%' order by 1,2;

/*looking at the total cases and population
#shows what percentage of population got covid*/

select location,date,population,total_cases,(total_cases/population)*100 as population_percentage_infected
from coviddeaths 
where location like '%india%'
 order by 1,2;
 
 /*countries with highest infection rate compared to population*/
 
 select location,population,max(cast(total_cases as double)) as highest_infection_count, 
 max(total_cases/population)*100 as percentage_population_infected
 from coviddeaths 
 #where location like'%india%' 
 group by location,population
 order by percentage_population_infected desc;
 
 /*countries with highest death count per population*/
 
 select location,max(total_deaths)as highest_total_deaths 
 from coviddeaths  
 where continent != '' 
 group by location
 order by highest_total_deaths desc;

/*now by continent of highest deaths
#selected location instead of continent because location has all the continents to show proper result*/

 select location,max(total_deaths)as highest_total_deaths 
 from coviddeaths  
 where continent = '' 
 group by location
 order by highest_total_deaths desc;
 
 /*global numbers
 #deaths_percentage*/ 
 
 select 
 sum(new_cases) as total_cases,sum(new_deaths)as total_deaths,
 sum(new_deaths)/sum(new_cases)*100 as death_percentage
 from coviddeaths
 -- where location like '%india%'
 where continent != ''
 group by date
 order by 1,2;
 
 /*total_populations vs total vaccinations
 Shows Percentage of Population that has recieved at least one Covid Vaccine
 and join
 */
 select d.continent,d.location,d.date,d.population,v.new_vaccinations,
 sum(v.new_vaccinations) over (partition by d.location order by d.location,d.date) as rolling_people_vaccinated
 from coviddeaths d
 join covidvaccinations v
 on d.location=v.location
 and d.date = v.date
# where d.location like '%india%'
 where d.continent != ''
 order by 2,3; 
 
  drop procedure if exists total_cases_vs_total_vac;       
delimiter //
create procedure total_cases_vs_total_vac(in _year int,in _country text)
	begin
	 select d.continent,d.location,d.date,d.total_cases,v.total_vaccinations
 
 from coviddeaths d
 join covidvaccinations v
 on d.location=v.location
 and d.date = v.date
 where
 year(d.date)=_year and
 d.location=_country 
 and
 d.continent != ''
 #and d.date like '%2023-01-01%'
 ;
	end//
delimiter ;

call my_portfolio_project.total_cases_vs_total_vac();
 
/*Using CTE to perform Calculation on Partition By in previous query
*/

With Pop_vs_Vac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
Select d.continent, d.location, 
d.date,
 d.population, v.new_vaccinations
, SUM(v.new_vaccinations) over (Partition by d.Location Order by d.location, d.Date) as RollingPeopleVaccinated
#, (RollingPeopleVaccinated/population)*100
From CovidDeaths d
Join CovidVaccinations v
	On d.location = v.location
	and d.date = v.date
#where d.continent != '' 
where d.location like '%nepal%'
)
Select *, (RollingPeopleVaccinated/Population)*100
From Pop_vs_Vac;
 
 
 -- Using Temporary Table to perform Calculation on Partition By in previous query

DROP Table if exists PercentPopulationVaccinated;
Create temporary Table PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date date,
Population numeric,
New_vaccinations text null,
RollingPeopleVaccinated text null
);

Insert into PercentPopulationVaccinated
Select d.continent, d.location, d.date, d.population, v.new_vaccinations
, SUM(v.new_vaccinations) OVER (Partition by d.Location Order by d.location, d.Date) as RollingPeopleVaccinated
From CovidDeaths d
Join my_portfolio_project.CovidVaccinations v
	On d.location = v.location
	and d.date = v.date
-- where dea.continent is not null 
-- order by 2,3
;

Select *, (RollingPeopleVaccinated/Population)*100
From PercentPopulationVaccinated;



-- Creating View to store data for later visualizations

Create View Percent_Population_Vaccinated as
Select d.continent, d.location, d.date, d.population, v.new_vaccinations
, SUM(v.new_vaccinations) OVER (Partition by d.Location Order by d.location, d.Date) as RollingPeopleVaccinated
-- (RollingPeopleVaccinated/population)*100
From CovidDeaths d
Join CovidVaccinations v
	On d.location = v.location
	and d.date = v.date
where d.continent !=''
#where location like '%india%'
;

SELECT * FROM my_portfolio_project.percent_population_vaccinated;

/*total deaths in india*/

drop view if exists total_deaths_india;
create view total_deaths_india as
select continent, location,sum(total_deaths) as total_deaths_so_far
from coviddeaths
where location like '%india%'
and date like '%2023-04-06%';
SELECT * FROM my_portfolio_project.total_deaths_india;

/*total cases of each countries as of till 2023
*/
drop view if exists total_cases_by_country;
create view total_cases_by_country as
select continent, location,date,total_cases as total_cases
from coviddeaths
where date like '%2023-01-01%'
and continent != '';

SELECT * FROM my_portfolio_project.total_cases_by_country;

/*total deaths of all countries as of 2023
*/
drop view if exists total_deaths_by_country;
create view total_deaths_by_country as
select continent, location,date,total_deaths as total_deaths
from coviddeaths
where date like '%2023-01-01%'
and continent != '';

SELECT * FROM my_portfolio_project.total_deaths_by_country;

/*a stored procedure to show total case vs total vaccination
as per year and country
*/



/*total cases*/
drop view if exists total_cases_world;
create view total_cases_world as
select total_cases
from coviddeaths
where date like '%2023-01-01%'
and location like '%world%';


SELECT * FROM my_portfolio_project.total_cases_world;


/*total deaths*/
drop view if exists total_deaths_world;
create view total_deaths_world as
select total_deaths
from coviddeaths
where date like '%2023-01-01%'
and location like '%world%';


SELECT * FROM my_portfolio_project.total_deaths_world;

/*total vaccinations*/

drop view if exists total_vac_world;
create view total_vac_world as
select total_vaccinations
from covidvaccinations
where date like '%2023-01-01%'
and location like '%world%';


SELECT * FROM my_portfolio_project.total_vac_world;
