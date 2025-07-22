# values = [None,0,0.0,'',[],{},set(),'False',1,[0],True,'sujan']
# for val in values:
#     print(f"{repr(val)} is {bool(val)} => {type(val)} => {bool(val)}")

# x = 10
# def modify(val):
#     global x
#     x = x+5
#     return x

# print(modify(x))
# print(x)

a = [1, 2, 3]
b = a
c = list(a)

print(a is b) 
print(a is c) 
print(a == c) 

