'''
print("Hello World")


name = input("enter name -")
print(name)
'''

"""
a = "Nuthan"
b = "Nuthan"
if a==b:
    print("they are equal")
"""
'''
str = "Naveen"
print(str[1:])
print(str[1:4])
print(str[-1])
print(str[-1:-10])
print(str[-3:-4])
print(str[:3])
print(str[:10])
print(str[10:])
print(str[:-4])
print(str[-4])
print(str[-4:])
print(str[4:])


x = int(input("Enter x -"))
y = int(input("Enter y -"))
sum = x+y
print(sum)

reverse(),sort() are inplace algorithms


l = ["abc","bha", "nuth", "bhb"]
print(l)
l.sort()
print(l)
print(l.reverse())
'''

z=[10]


def globalVarTes():
    z[0] = 20
    print(z[0])


def f(z):
    globalVarTes()
    return z[0]*z[0]


print(f(z))


