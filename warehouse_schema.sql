-- this script sets of the DB2 database, i.e. creates the tables and inserts data
DROP TABLE SalesFact CASCADE;
DROP TABLE ProductDim CASCADE;
DROP TABLE LocationDim CASCADE;
DROP TABLE TimeDim CASCADE;

CREATE TABLE ProductDim (
  ProductID int NOT NULL,
  Category varchar(255) NOT NULL,
  Family   varchar(255) NOT NULL,
  Class    varchar(255) NOT NULL,
  Price    double precision NOT NULL,
  PRIMARY KEY (ProductID)
);

CREATE TABLE LocationDim (
  LocationID int NOT NULL ,
  Land   varchar(255) NOT NULL,
  Region varchar(255) NOT NULL,
  State  varchar(255) NOT NULL,
  Shop   varchar(255) NOT NULL,
  PRIMARY KEY (LocationID)
);

CREATE TABLE TimeDim (
  TimeID int NOT NULL ,
  Day      integer NOT NULL,
  Month    integer NOT NULL,
  Year     integer NOT NULL,
  Quarter  integer NOT NULL,
  PRIMARY KEY (TimeID)
);

CREATE TABLE SalesFact (
  ProductID  integer NOT NULL,
  LocationID integer NOT NULL,
  TimeID     integer NOT NULL,
  SalesUnit  integer NOT NULL,
  Profit     double precision NOT NULL
);

ALTER TABLE SalesFact ADD CONSTRAINT ProductID_fk_1 FOREIGN KEY (ProductID) REFERENCES ProductDim (ProductID);

ALTER TABLE SalesFact ADD CONSTRAINT LocationID_fk_1 FOREIGN KEY (LocationID) REFERENCES LocationDim (LocationID);

ALTER TABLE SalesFact ADD CONSTRAINT TimeID_fk_1 FOREIGN KEY (TimeID) REFERENCES TimeDim (TimeID);

ALTER TABLE SalesFact ADD PRIMARY KEY (ProductID, LocationID, TImeID)
