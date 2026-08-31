from dataclasses import dataclass


@dataclass(frozen=True)
class TableSpec:
    target_table: str
    source_query: str
    target_columns: tuple[str, ...]


TABLE_SPECS = (
    TableSpec(
        target_table="customers",
        source_query="""
            SELECT
                RTRIM(CustomerID),
                CompanyName,
                ContactName,
                ContactTitle,
                Address,
                City,
                Region,
                PostalCode,
                Country,
                Phone,
                Fax
            FROM dbo.Customers
            ORDER BY CustomerID
        """,
        target_columns=(
            "customer_id",
            "company_name",
            "contact_name",
            "contact_title",
            "address",
            "city",
            "region",
            "postal_code",
            "country",
            "phone",
            "fax",
        ),
    ),
    TableSpec(
        target_table="employees",
        source_query="""
            SELECT
                EmployeeID,
                LastName,
                FirstName,
                Title,
                TitleOfCourtesy,
                BirthDate,
                HireDate,
                Address,
                City,
                Region,
                PostalCode,
                Country,
                HomePhone,
                Extension,
                Photo,
                Notes,
                ReportsTo,
                PhotoPath
            FROM dbo.Employees
            ORDER BY EmployeeID
        """,
        target_columns=(
            "employee_id",
            "last_name",
            "first_name",
            "title",
            "title_of_courtesy",
            "birth_date",
            "hire_date",
            "address",
            "city",
            "region",
            "postal_code",
            "country",
            "home_phone",
            "extension",
            "photo",
            "notes",
            "reports_to",
            "photo_path",
        ),
    ),
    TableSpec(
        target_table="suppliers",
        source_query="""
            SELECT
                SupplierID,
                CompanyName,
                ContactName,
                ContactTitle,
                Address,
                City,
                Region,
                PostalCode,
                Country,
                Phone,
                Fax,
                HomePage
            FROM dbo.Suppliers
            ORDER BY SupplierID
        """,
        target_columns=(
            "supplier_id",
            "company_name",
            "contact_name",
            "contact_title",
            "address",
            "city",
            "region",
            "postal_code",
            "country",
            "phone",
            "fax",
            "home_page",
        ),
    ),
    TableSpec(
        target_table="categories",
        source_query="""
            SELECT
                CategoryID,
                CategoryName,
                Description,
                Picture
            FROM dbo.Categories
            ORDER BY CategoryID
        """,
        target_columns=(
            "category_id",
            "category_name",
            "description",
            "picture",
        ),
    ),
    TableSpec(
        target_table="products",
        source_query="""
            SELECT
                ProductID,
                ProductName,
                SupplierID,
                CategoryID,
                QuantityPerUnit,
                UnitPrice,
                UnitsInStock,
                UnitsOnOrder,
                ReorderLevel,
                Discontinued
            FROM dbo.Products
            ORDER BY ProductID
        """,
        target_columns=(
            "product_id",
            "product_name",
            "supplier_id",
            "category_id",
            "quantity_per_unit",
            "unit_price",
            "units_in_stock",
            "units_on_order",
            "reorder_level",
            "discontinued",
        ),
    ),
    TableSpec(
        target_table="shippers",
        source_query="""
            SELECT
                ShipperID,
                CompanyName,
                Phone
            FROM dbo.Shippers
            ORDER BY ShipperID
        """,
        target_columns=(
            "shipper_id",
            "company_name",
            "phone",
        ),
    ),
    TableSpec(
        target_table="regions",
        source_query="""
            SELECT
                RegionID,
                RTRIM(RegionDescription)
            FROM dbo.Region
            ORDER BY RegionID
        """,
        target_columns=(
            "region_id",
            "region_description",
        ),
    ),
    TableSpec(
        target_table="territories",
        source_query="""
            SELECT
                TerritoryID,
                RTRIM(TerritoryDescription),
                RegionID
            FROM dbo.Territories
            ORDER BY TerritoryID
        """,
        target_columns=(
            "territory_id",
            "territory_description",
            "region_id",
        ),
    ),
    TableSpec(
        target_table="employee_territories",
        source_query="""
            SELECT
                EmployeeID,
                TerritoryID
            FROM dbo.EmployeeTerritories
            ORDER BY EmployeeID, TerritoryID
        """,
        target_columns=(
            "employee_id",
            "territory_id",
        ),
    ),
    TableSpec(
        target_table="orders",
        source_query="""
            SELECT
                OrderID,
                RTRIM(CustomerID),
                EmployeeID,
                OrderDate,
                RequiredDate,
                ShippedDate,
                ShipVia,
                Freight,
                ShipName,
                ShipAddress,
                ShipCity,
                ShipRegion,
                ShipPostalCode,
                ShipCountry
            FROM dbo.Orders
            ORDER BY OrderID
        """,
        target_columns=(
            "order_id",
            "customer_id",
            "employee_id",
            "order_date",
            "required_date",
            "shipped_date",
            "ship_via",
            "freight",
            "ship_name",
            "ship_address",
            "ship_city",
            "ship_region",
            "ship_postal_code",
            "ship_country",
        ),
    ),
    TableSpec(
        target_table="order_details",
        source_query="""
            SELECT
                OrderID,
                ProductID,
                UnitPrice,
                Quantity,
                Discount
            FROM dbo.[Order Details]
            ORDER BY OrderID, ProductID
        """,
        target_columns=(
            "order_id",
            "product_id",
            "unit_price",
            "quantity",
            "discount",
        ),
    ),
)
