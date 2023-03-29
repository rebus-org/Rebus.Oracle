using System;

namespace Rebus.Oracle.Schema;

/// <summary>Represents an object name, either in [Owner.]Name form.</summary>
/// <remarks>Escaped names (between double quotes) are not supported.</remarks>
public class DbName 
{
    /// <summary>Owner of the object, or null if unqualified.</summary>
    public string Owner { get; }
    /// <summary>Name of the object.</summary>
    public string Name { get; }
    internal string Prefix { get; }

    private readonly string _full;

    /// <summary></summary>
    /// <param name="qualifiedName">A full object name, qualified or not.</param>
    public DbName(string qualifiedName)
    {
        if (qualifiedName == null) throw new ArgumentNullException(nameof(qualifiedName));
            
        _full = qualifiedName;

        var parts = qualifiedName.Split(new[] { '.' }, 2);
        if (parts.Length == 2) 
        {
            Owner = parts[0];
            Name = parts[1];
            Prefix = Owner + '.';
        }
        else
        {
            Owner = null;
            Name = qualifiedName;
            Prefix = "";
        }            
    }

    /// <summary></summary>
    /// <param name="owner">Owner of the object.</param>
    /// <param name="name">Name of the object.</param>
    public DbName(string owner, string name)
    {
        Owner = owner;
        Name = name;
        Prefix = owner != null ? owner + '.' : null;
        _full = Prefix + Name;
    }

    /// <inheritdoc />
    public override string ToString() => _full;
}