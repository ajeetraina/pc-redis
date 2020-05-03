import redis
# Create connection object
r = redis.Redis(host='localhost', port=6379)

def set_counters(r):

    r.set('counter:categories',0)
    r.set('counter:products',0)


def create_category(r,name):
    category = {}

    category_counter = r.incr('counter:categories')
    
    category['idx'] = category_counter
    category['name'] = name
    r.hmset('category:'+str(category_counter),category)

    r.sadd('categories:all','category:'+str(category_counter))

def get_category(r,id):
    print(r.hgetall("category:"+str(id)))
        
def update_category(r,id,name):
    category = {}
    category['idx'] = id
    category['name'] = name
    r.hmset('category:'+str(id),category)



def create_product(r,product):
    
    if r.exists('category:'+str(product['MainCategoryID'])):

        product_counter = r.incr('counter:products')
        
        product['idx'] = product_counter
        
        r.hmset('product:'+str(product_counter),product)
        
        r.sadd('categories:'+str(product['MainCategoryID']+":products")
        ,'product:'+str(product_counter))

        r.sadd('products:all','product:'+str(product_counter))

    else:
        print("Product Main Category {0} doesnt Exists".format(product['MainCategoryID']))

def update_product(r,id,product):
    product['idx'] = id
    
    old_category = r.hget('product:'+str(id),'MainCategoryID')

    r.hmset('product:'+str(id),product)

    r.srem('categories:'+str(old_category+":products")
        ,'product:'+str(id))

    r.sradd('categories:'+str(product['MainCategoryID']+":products")
        ,'product:'+str(id))
    

def delete_product(r,id):
    
    # Get the Main Category ID and Update the Categories Set

    category_id = r.hget('product:'+str(id),'MainCategoryID')

    r.srem('category:'+str(category_id)+":products",'product'+str(id))

    r.srem('products:all','product:'+str(id))
    
    r.delete('product:'+str(id))


def get_product(r,id):
    print(r.hgetall("product:"+str(id)))

def get_all_products(r):
    products_list = r.smembers("products:all")

    for product in products_list:
        print(r.hgetall(product))




# Initialize the Counters
set_counters(r)

create_category(r,'tv') 
create_category(r,'laptop') 
create_category(r,'mobile')

get_category(r,1)
get_category(r,2)
get_category(r,3)

create_product(r,{
    'name':'iphoneX',
    'description':'Power Mobile','vendor':'Apple',
    'price':'110000','currency':'INR','MainCategoryID':'3'}
    )

create_product(r,{
    'name':'Latitude',
    'description':'Tough Laptop','vendor':'Dell',
    'price':'72000','currency':'INR','MainCategoryID':'2'}
    )

create_product(r,{
    'name':'Redmi Note 5',
    'description':'Good Camera Mobile','vendor':'Xiaomi',
    'price':'15000','currency':'INR','MainCategoryID':'3'}
    )

create_product(r,{
    'name':'Samsung 42 TV',
    'description':'Wide TV','vendor':'Samsung',
    'price':'33000','currency':'INR','MainCategoryID':'1'}
    )

create_product(r,{
    'name':'LG Plasma 55 TV',
    'description':'Best Plasma TV','vendor':'LG',
    'price':'90000','currency':'INR','MainCategoryID':'5'}
    )

create_product(r,{
    'name':'LG Plasma 55 TV',
    'description':'Best Plasma TV','vendor':'LG',
    'price':'90000','currency':'INR','MainCategoryID':'1'}
    )


#get_product(r,4)

#get_all_products(r)

print("######### Displaying All the Products ############")

get_all_products(r)

print("################################")

delete_product(r,5)

print("######### Displaying All the Products ############")

get_all_products(r)

print("#################################")